package main

import (
	"log"
	"os"

	"github.com/gin-gonic/gin"

	"github.com/Shreyas9468/fluxqueue/api"
	"github.com/Shreyas9468/fluxqueue/redis"
)

// Research:
// - How to create a Gin engine (gin.Default())
// - How to read REDIS_ADDR from env (same as worker)
// - How to create RedisStore once and pass it to handlers
// - How to start the server (r.Run(":8080"))
// - Graceful shutdown is optional for now
func main() {

	router := gin.Default()
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	store := redis.NewRedisStore(redisAddr)
	api.SetupRouter(store, router)
	addr := getEnv("HTTP_ADDR", ":8080")
	if err := router.Run(addr); err != nil {
		log.Fatal(err)
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
