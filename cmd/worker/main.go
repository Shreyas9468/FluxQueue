package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shreyas9468/fluxqueue/redis"
	"github.com/Shreyas9468/fluxqueue/worker"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	visibilityTimeout := 10 * time.Second

	store := redis.NewRedisStore(redisAddr)
	w := worker.NewWorker(store, visibilityTimeout)

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	log.Println("Starting FluxQueue worker...")
	log.Printf("Redis: %s | Visibility Timeout: %v",
		redisAddr, visibilityTimeout)

	if err := w.Start(ctx); err != nil {
		log.Fatalf("Worker stopped with error: %v", err)
	}

	log.Println("Worker stopped gracefully")
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
