package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrWaitTimeout = errors.New("cache wait timeout")

// WaitUntilCached polls key until the lock holder populates it or maxWait elapses.
// Call this when TryAcquireLock returns acquired == false.
func WaitUntilCached(ctx context.Context, client redis.UniversalClient, key string, maxWait, poll time.Duration) ([]byte, error) {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		data, err := client.Get(ctx, key).Bytes()
		if err == nil {
			return data, nil
		}
		if !errors.Is(err, redis.Nil) {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(poll):
		}
	}
	return nil, ErrWaitTimeout
}

func randomToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
