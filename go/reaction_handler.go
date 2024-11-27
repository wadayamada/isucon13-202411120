package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

type ReactionModel struct {
	ID           int64  `db:"id"`
	EmojiName    string `db:"emoji_name"`
	UserID       int64  `db:"user_id"`
	LivestreamID int64  `db:"livestream_id"`
	CreatedAt    int64  `db:"created_at"`
}

type Reaction struct {
	ID         int64      `json:"id"`
	EmojiName  string     `json:"emoji_name"`
	User       User       `json:"user"`
	Livestream Livestream `json:"livestream"`
	CreatedAt  int64      `json:"created_at"`
}

type PostReactionRequest struct {
	EmojiName string `json:"emoji_name"`
}

func getReactionsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	query := "SELECT * FROM reactions WHERE livestream_id = ? ORDER BY created_at DESC"
	if c.QueryParam("limit") != "" {
		limit, err := strconv.Atoi(c.QueryParam("limit"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "limit query parameter must be integer")
		}
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	reactionModels := []ReactionModel{}
	if err := tx.SelectContext(ctx, &reactionModels, query, livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "failed to get reactions")
	}
	reactions, err := fillReactionResponse(ctx, tx, reactionModels)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill reaction: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusOK, reactions)
}

func postReactionHandler(c echo.Context) error {
	ctx := c.Request().Context()
	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var req *PostReactionRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	reactionModel := ReactionModel{
		UserID:       int64(userID),
		LivestreamID: int64(livestreamID),
		EmojiName:    req.EmojiName,
		CreatedAt:    time.Now().Unix(),
	}

	result, err := tx.NamedExecContext(ctx, "INSERT INTO reactions (user_id, livestream_id, emoji_name, created_at) VALUES (:user_id, :livestream_id, :emoji_name, :created_at)", reactionModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert reaction: "+err.Error())
	}

	reactionID, err := result.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted reaction id: "+err.Error())
	}
	reactionModel.ID = reactionID
	reactionModels := []ReactionModel{}
	reactionModels = append(reactionModels, reactionModel)

	reactions, err := fillReactionResponse(ctx, tx, reactionModels)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill reaction: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusCreated, reactions[0])
}

func fillReactionResponse(ctx context.Context, tx *sqlx.Tx, reactionModels []ReactionModel) ([]Reaction, error) {
	reactionOwnerModels := []UserModel{}
	userIds := make([]int64, len(reactionModels))
	livestreamIds := make([]int64, len(reactionModels))
	for i, reactionModel := range reactionModels {
		userIds[i] = reactionModel.UserID
		livestreamIds[i] = reactionModel.LivestreamID
	}
	if len(userIds) != 0 {
		rawSql := "SELECT * FROM users WHERE id IN (?)"
		sql, args, _ := sqlx.In(rawSql, userIds)
		if err := tx.SelectContext(ctx, &reactionOwnerModels, sql, args...); err != nil {
			log.Error("failed fillReactionResponse: ", err)
			return []Reaction{}, err
		}
	}
	reactionOwnerMap, err := fillUserResponseV2(ctx, tx, reactionOwnerModels)
	if err != nil {
		log.Error("failed fillReactionResponse: ", err)
		return []Reaction{}, err
	}
	livestreamModels := []*LivestreamModel{}
	livestreamIdToLivestreamMap := make(map[int64]Livestream)
	if len(livestreamIds) != 0 {
		sql, args, _ := sqlx.In("SELECT * FROM livestreams WHERE id IN (?)", livestreamIds)
		if err := tx.SelectContext(ctx, &livestreamModels, sql, args...); err != nil {
			log.Error("failed fillReactionResponse: ", err)
			return []Reaction{}, err
		}
		livestreams, err := fillLivestreamResponse(ctx, tx, livestreamModels)
		if err != nil {
			log.Error("failed fillReactionResponse: ", err)
			return []Reaction{}, err
		}
		for _, livestream := range livestreams {
			livestreamIdToLivestreamMap[livestream.ID] = livestream
		}
	}
	reactions := []Reaction{}
	for _, reactionModel := range reactionModels {
		reaction := Reaction{
			ID:         reactionModel.ID,
			EmojiName:  reactionModel.EmojiName,
			User:       reactionOwnerMap[reactionModel.UserID],
			Livestream: livestreamIdToLivestreamMap[reactionModel.LivestreamID],
			CreatedAt:  reactionModel.CreatedAt,
		}
		reactions = append(reactions, reaction)
	}

	return reactions, nil
}
