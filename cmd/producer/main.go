package main

import (
	"context"
	"log"
	"os"

	"github.com/Shreyas9468/fluxqueue/job"
	"github.com/Shreyas9468/fluxqueue/redis"
)

func main() {
	redisAdrr := getEnv("REDIS_ADDR", "localhost:6379")
	store := redis.NewRedisStore(redisAdrr)

	ctx := context.Background()
	job := &job.Job{
		PayLoad:     []byte("Hello World"),
		Maxattempts: 3,
	}
	if err := store.Enqueue(ctx, job); err != nil {
		log.Fatalf("failed to enqueue job: %v", err)
	}

	log.Printf("Job enqueued successfully | ID: %s", job.ID)
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
