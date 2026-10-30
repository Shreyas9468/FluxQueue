package api

import (
	"github.com/gin-gonic/gin"

	"github.com/Shreyas9468/fluxqueue/redis"
)

func SetupRouter(store *redis.RedisStore, r *gin.Engine) {
	h := &Handler{rds: store}

	r.GET("/health", h.Health)
	r.POST("/jobs", h.EnqueueJob)
}
