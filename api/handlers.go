package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Shreyas9468/fluxqueue/job"
	"github.com/Shreyas9468/fluxqueue/redis"
)

type Handler struct {
	rds *redis.RedisStore
}

type EnqueueRequest struct {
	Type    string `json:"type" binding:"required"`
	Payload string `json:"payload"`
}

func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
	})
}

func (h *Handler) EnqueueJob(c *gin.Context) {
	var req EnqueueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	j := &job.Job{
		Type:        req.Type,
		PayLoad:     []byte(req.Payload),
		Maxattempts: 3,
	}

	if err := h.rds.Enqueue(c.Request.Context(), j); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id": j.ID,
	})
}
