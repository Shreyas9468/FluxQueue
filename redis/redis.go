package redis

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	rds "github.com/redis/go-redis/v9"

	"github.com/Shreyas9468/fluxqueue/job"
)

type RedisStore struct {
	client *rds.Client
}

func NewRedisStore(addr string) *RedisStore {
	ctx := context.Background()
	rdc := rds.NewClient(&rds.Options{
		Addr:     addr,
		Password: "", // no password set
		DB:       0,  // use default DB
		PoolSize: 10, // this is defualt for go-redis

	})
	pong, err := rdc.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect to redis :%v", err)
	}
	log.Printf("Connected to redis %s", pong)
	return &RedisStore{
		client: rdc,
	}

}

// Fail is called when process() returns an error.
// Steps:
// 1. Remove job from queue:processing
// 2. Load metadata, increment attempts
// 3. If attempts < max_attempts → add to queue:delayed with score = now + delay
// 4. If attempts >= max_attempts → push to queue:dlq
// Research:
//   - How to calculate delay (fixed 10s or simple exponential)
//   - ZADD for sorted set
//   - What to store as the member (just job ID is enough)
func (s *RedisStore) Fail(ctx context.Context, jobID string) error {
	// TODO: Implement

	const (
		processingKey = "queue:processing"
		delayedKey    = "queue:delayed"
		dlqKey        = "queue:dlq"
	)

	err := s.client.LRem(ctx, processingKey, 1, jobID).Err()

	if err != nil {
		return fmt.Errorf("Failed to remove job from processing queue : %s", jobID)
	}

	metaKey := fmt.Sprintf("job:%s", jobID)
	attempts, err := s.client.HIncrBy(ctx, metaKey, "attempts", 1).Result()
	if err != nil {
		return fmt.Errorf("failed to increment attempts: %w", err)
	}
	maxAttemptsStr, err := s.client.HGet(ctx, metaKey, "max_attempts").Result()
	if err != nil {
		return fmt.Errorf("failed to get max_attempts: %w", err)
	}
	var maxAttempts int64
	fmt.Sscanf(maxAttemptsStr, "%d", &maxAttempts)

	// 4. Decide: retry or DLQ
	if attempts < maxAttempts {
		// Calculate delay (simple exponential in seconds)
		// 1st retry: 5s, 2nd: 10s, 3rd: 20s ...
		delaySeconds := (1 << (attempts - 1)) * 5
		jitter := rand.Intn(25) // 25% jitter
		delaySeconds += jitter
		if delaySeconds > 80 {
			delaySeconds = 80 // cap at 1.2 minute
		}

		readyAt := time.Now().UTC().Add(time.Duration(delaySeconds) * time.Second).Unix()

		// Add to delayed sorted set (score = when it should become ready)
		err = s.client.ZAdd(ctx, delayedKey, rds.Z{
			Score:  float64(readyAt),
			Member: jobID,
		}).Err()
		if err != nil {
			return fmt.Errorf("failed to add job to delayed queue: %w", err)
		}

		// Clear claimed_at
		_ = s.client.HSet(ctx, metaKey, "claimed_at", "").Err()

		log.Printf("job %s scheduled for retry in %ds (attempt %d/%d)", jobID, delaySeconds, attempts, maxAttempts)
	} else {
		// Move to DLQ
		err = s.client.LPush(ctx, dlqKey, jobID).Err()
		if err != nil {
			return fmt.Errorf("failed to move job to DLQ: %w", err)
		}

		log.Printf("job %s moved to DLQ after %d attempts", jobID, attempts)
	}

	return nil
}

func (s *RedisStore) PromoteDelayed(ctx context.Context) (int, error) {
	// TODO:
	// 1. Get current unix timestamp
	// 2. ZRANGEBYSCORE queue:delayed -inf <now>
	// 3. For each job ID:
	//      - ZREM from delayed
	//      - LPUSH into queue:ready
	// 4. Return how many jobs were promoted
	const (
		delayedKey = "queue:delayed"
		readyKey   = "queue:ready"
	)

	now := time.Now().UTC().Unix()
	jobIDs, err := s.client.ZRangeArgs(ctx, rds.ZRangeArgs{
		Key:     delayedKey,
		Start:   "-inf",
		Stop:    now,
		ByScore: true,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get jobs from delayed queue: %w", err)
	}

	if len(jobIDs) == 0 {
		return 0, nil
	}

	pipe := s.client.Pipeline()

	for _, jobID := range jobIDs {
		pipe.ZRem(ctx, delayedKey, jobID) // remove from delayed
		pipe.LPush(ctx, readyKey, jobID)  // push to ready
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to promote delayed jobs: %w", err)
	}

	return len(jobIDs), nil

}

// Enqueue adds a job to the ready queue.
// Research: Which Redis command to use for adding to a list? (LPUSH / RPUSH)
// Also decide: should job metadata be stored in a Hash?
func (s *RedisStore) Enqueue(ctx context.Context, j *job.Job) error {
	// TODO: Implement
	const readyKey = "queue:ready"
	if j.ID == "" {
		j.ID = uuid.New().String()
	}

	now := time.Now().UTC()
	if j.Created_at.IsZero() {
		j.Created_at = now
	}

	if j.Maxattempts == 0 {
		j.Maxattempts = 3
	}

	metaKey := fmt.Sprintf("job:%s", j.ID)

	err := s.client.HSet(ctx, metaKey, map[string]interface{}{
		"id":           j.ID,
		"payload":      string(j.PayLoad),
		"attempts":     j.Attempts,
		"max_attempts": j.Maxattempts,
		"created_at":   j.Created_at.Format(time.RFC3339),
		"claimed_at":   "",
	}).Err()
	if err != nil {
		return fmt.Errorf("Failed to store job meta data : %v", err)
	}
	err = s.client.LPush(ctx, readyKey, j.ID).Err()
	if err != nil {
		_ = s.client.Del(ctx, metaKey).Err()
		return fmt.Errorf("Failed to enque the job : %v", err)
	}
	return nil
}

// Claim atomically moves one job from ready → processing.
// Research: BRPOPLPUSH or BLMOVE
// This is the most important function. Make sure it is atomic.
func (s *RedisStore) Claim(ctx context.Context, timeout time.Duration) (*job.Job, error) {
	// TODO: Implement atomic claim
	// - Move job from ready list to processing list
	// - Load job metadata
	// - Set ClaimedAt
	const (
		readyKey      = "queue:ready"
		processingKey = "queue:processing"
	)
	jobId, err := s.client.BLMove(ctx, readyKey, processingKey, "RIGHT", "LEFT", timeout).Result()

	if err != nil {

		if err == rds.Nil {
			// Timeout — no job available
			return nil, nil
		}

		return nil, fmt.Errorf("Failed to claim a job : %v", err)
	}

	metakey := fmt.Sprintf("job:%s", jobId)
	data, err := s.client.HGetAll(ctx, metakey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to load job metadata: %w", err)
	}
	if len(data) == 0 {
		// Job metadata missing (should not happen normally)
		// You may want to remove this ID from processing list here
		return nil, fmt.Errorf("job metadata not found for id %s", jobId)
	}
	j := &job.Job{
		ID: jobId,
	}
	if payload, ok := data["payload"]; ok {
		j.PayLoad = []byte(payload)
	}
	if attempts, ok := data["attempts"]; ok {
		fmt.Sscanf(attempts, "%d", &j.Attempts)
	}
	if maxAttempts, ok := data["max_attempts"]; ok {
		fmt.Sscanf(maxAttempts, "%d", &j.Maxattempts)
	}

	now := time.Now().UTC()
	j.Claimed_at = now
	j.Created_at, err = time.Parse(time.RFC3339, data["created_at"])
	if err != nil {
		return nil, fmt.Errorf("failed to parse created_at: %w", err)
	}
	err = s.client.HSet(ctx, metakey, "claimed_at", now.Format(time.RFC3339)).Err()
	if err != nil {
		return nil, fmt.Errorf("failed to set claimed_at: %w", err)
	}

	return j, nil
}

// Ack removes a successfully processed job from the processing list.
// Research: How to safely remove a specific job from a list + delete its metadata.
func (s *RedisStore) Ack(ctx context.Context, jobID string) error {
	const processingKey = "queue:processing"
	metakey := fmt.Sprintf("job:", jobID)
	//    LREM key count value  → count=1 means remove one occurrence
	removed, err := s.client.LRem(ctx, processingKey, 1, jobID).Result()
	if err != nil {
		return fmt.Errorf("failed to remove job from processing list: %w", err)
	}
	if removed == 0 {
		// Job was not in the processing list (maybe already reclaimed or acked)
		// You can decide whether to treat this as an error or just log it
		return fmt.Errorf("job %s was not found in processing list", jobID)
	}
	err = s.client.Del(ctx, metakey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete job metadata: %w", err)
	}
	return nil
}

// ReclaimExpired moves jobs that have been in processing longer than visibilityTimeout
// back to the ready queue.
// Research: How to scan the processing list and check timestamps.
// This is the key to handling worker crashes.
func (s *RedisStore) ReclaimExpired(ctx context.Context, visibilityTimeout time.Duration) (int, error) {

	const (
		processingKey = "queue:processing"
		readyKey      = "queue:ready"
	)

	jobIDs, err := s.client.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to read processing list: %w", err)
	}
	reclaimed := 0
	now := time.Now().UTC()

	for _, jobID := range jobIDs {
		metaKey := fmt.Sprintf("job:%s", jobID)

		// 2. Get the claimed_at timestamp
		claimedAtStr, err := s.client.HGet(ctx, metaKey, "claimed_at").Result()
		if err == rds.Nil || claimedAtStr == "" {
			// No claimed_at → treat as expired (or orphaned)
			// Move it back to ready
		} else if err != nil {
			continue // skip on error
		} else {
			claimedAt, err := time.Parse(time.RFC3339, claimedAtStr)
			if err != nil {
				continue
			}

			// 3. Check if the lease has expired
			if now.Sub(claimedAt) < visibilityTimeout {
				continue // still within visibility window
			}
		}

		// 4. Job is expired → move it back to ready queue
		//    We use a pipeline to make it safer
		pipe := s.client.Pipeline()

		// Remove from processing
		pipe.LRem(ctx, processingKey, 1, jobID)

		// Push back to ready
		pipe.LPush(ctx, readyKey, jobID)

		// Clear claimed_at so the next claim starts fresh
		pipe.HSet(ctx, metaKey, "claimed_at", "")

		// Optionally increment attempts here if you want
		// pipe.HIncrBy(ctx, metaKey, "attempts", 1)

		_, err = pipe.Exec(ctx)
		if err != nil {
			continue
		}

		reclaimed++
	}

	return reclaimed, nil
}
