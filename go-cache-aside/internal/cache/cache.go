package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// SetCached stores data and signals waiters with an atomic SET + LPUSH.
func SetCached(ctx context.Context, client redis.UniversalClient, keys SlotKeys, value []byte, ttl time.Duration) error {
	_, err := setCacheScript.Run(ctx, client, []string{keys.Data, keys.Notify}, value, int64(ttl.Seconds())).Result()
	return err
}

// DeleteSlot removes cached data and any pending notify signals.
func DeleteSlot(ctx context.Context, client redis.UniversalClient, keys SlotKeys) error {
	return client.Del(ctx, keys.Data, keys.Notify).Err()
}
