package job

import "time"

type Job struct {
	ID          string
	Type        string
	PayLoad     []byte
	Attempts    int
	Maxattempts int
	Claimed_at  time.Time
	Created_at  time.Time
}
