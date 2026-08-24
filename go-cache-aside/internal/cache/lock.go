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

func TryAcquireLock(ctx context.Context, client redis.UniversalClient, lockKey string, ttl time.Duration) (token string, acquired bool, err error) {
	token, err = randomToken()
	if err != nil {
		return "", false, err
	}
	acquired, err = client.SetNX(ctx, lockKey, token, ttl).Result()
	if err != nil {
		return "", false, err
	}
	return token, acquired, nil
}

var releaseLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

func ReleaseLock(ctx context.Context, client redis.UniversalClient, lockKey, token string) error {
	return releaseLockScript.Run(ctx, client, []string{lockKey}, token).Err()
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
