package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Shreyas9468/fluxqueue/job"
	"github.com/Shreyas9468/fluxqueue/redis"
)

type Worker struct {
	store             *redis.RedisStore
	visibilityTimeout time.Duration
}

func NewWorker(store *redis.RedisStore, visibilityTimeout time.Duration) *Worker {

	return &Worker{
		store:             store,
		visibilityTimeout: visibilityTimeout,
	}
}

// Start begins the worker loop.
// Research: How should the main loop look?
// Typical pattern:
//
//	for {
//	  job = Claim()
//	  process(job)
//	  if success → Ack()
//	  else → handle failure (we will add later)
//	}
func (w *Worker) Start(ctx context.Context) error {

	log.Println("Worker started")

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n1, err := w.store.ReclaimExpired(ctx, w.visibilityTimeout)
				if err != nil {
					log.Printf("failed to reclaim expired jobs: %v", err)
					continue
				}
				if n1 > 0 {
					log.Printf("reclaimed %d expired jobs", n1)
				}
				n2, err := w.store.PromoteDelayed(ctx)
				if err != nil {
					log.Printf("promote delayed error: %v", err)
				}
				if n2 > 0 {
					log.Printf("promoted %d delayed jobs to ready", n2)
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("Worker shutting down")
			return nil
		default:
			j, err := w.store.Claim(ctx, 5*time.Second)
			if err != nil {
				log.Printf("failed to claim a job: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if j == nil {
				// time.Sleep(1 * time.Second)
				continue
			}
			log.Printf("Claimed job %s", j.ID)

			err = w.process(j)
			if err != nil {
				log.Printf("failed to process job %s: %v", j.ID, err)
				// Later we will add retry / DLQ logic here.
				if failerr := w.store.Fail(ctx, j.ID); failerr != nil {
					log.Printf("failed to mark job as failed %s: %v", j.ID, failerr)
				}
				continue
			}

			err = w.store.Ack(ctx, j.ID)
			if err != nil {
				log.Printf("failed to ack job %s: %v", j.ID, err)
				continue
			}
			log.Printf("Acked job %s", j.ID)
		}
	}
}

func (w *Worker) process(j *job.Job) error {

	// Simulate some work (remove this sleep later)
	time.Sleep(2 * time.Second)

	// For now always succeed.
	switch j.Type {
	case "echo":
		// Simple simulation of work
		log.Printf("processing job %s | payload: %s", j.ID, string(j.PayLoad))
	case "fail":
		return fmt.Errorf("failed to process job %s", j.ID)
	default:
		return fmt.Errorf("unknown job type %s", j.Type)
	}

	return nil
}
