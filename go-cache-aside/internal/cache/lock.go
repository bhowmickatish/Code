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

const lockKeyPrefix = "lock:"

func LockKey(cacheKey string) string {
	return lockKeyPrefix + cacheKey
}

// Lock is a Redis token lock acquired with SET NX EX.
type Lock struct {
	client redis.UniversalClient
	key    string
	token  string
}

// TryAcquireLock attempts SET key token NX EX ttl. On success, returns a Lock that must be released.
func TryAcquireLock(ctx context.Context, client redis.UniversalClient, lockKey string, ttl time.Duration) (lock *Lock, acquired bool, err error) {
	token, err := randomToken()
	if err != nil {
		return nil, false, err
	}
	acquired, err = client.SetNX(ctx, lockKey, token, ttl).Result()
	if err != nil {
		return nil, false, err
	}
	if !acquired {
		return nil, false, nil
	}
	return &Lock{client: client, key: lockKey, token: token}, true, nil
}

// Release deletes the lock key only when this holder's token still matches.
func (l *Lock) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	_, err := releaseLockScript.Run(ctx, l.client, []string{l.key}, l.token).Result()
	return err
}

func WaitForCache(ctx context.Context, client redis.UniversalClient, key string, maxWait, poll time.Duration) ([]byte, error) {
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
