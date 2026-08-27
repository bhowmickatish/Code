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

// WaitUntilCached blocks on BRPOP until the lock holder populates data or maxWait elapses.
// Call when TryAcquireLock returns acquired == false.
func WaitUntilCached(ctx context.Context, client redis.UniversalClient, keys SlotKeys, maxWait time.Duration) ([]byte, error) {
	data, err := client.Get(ctx, keys.Data).Bytes()
	if err == nil {
		return data, nil
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	_, err = client.BRPop(ctx, maxWait, keys.Notify).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrWaitTimeout
		}
		return nil, err
	}

	data, err = client.Get(ctx, keys.Data).Bytes()
	if err == nil {
		return data, nil
	}
	if errors.Is(err, redis.Nil) {
		return nil, ErrWaitTimeout
	}
	return nil, err
}

func randomToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
