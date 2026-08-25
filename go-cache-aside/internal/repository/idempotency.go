package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/atish/go-cache-aside/internal/cache"
	"github.com/atish/go-cache-aside/internal/model"
	"github.com/redis/go-redis/v9"
)

const idempotencyKeyPrefix = "idempotency:"

type idempotencyRecord struct {
	Name       string        `json:"name"`
	PriceCents int64         `json:"price_cents"`
	Product    model.Product `json:"product"`
}

// idempotencyFingerprint derives a stable Redis key from the create request body.
func idempotencyFingerprint(name string, priceCents int64) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%d", name, priceCents))
	return idempotencyKeyPrefix + hex.EncodeToString(sum[:])
}

func (r *ProductRepository) resolveIdempotency(ctx context.Context, idemKey string) (*model.Product, error) {
	data, err := r.cache.Get(ctx, idemKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("idempotency get: %w", err)
	}

	var rec idempotencyRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, nil
	}
	return &rec.Product, nil
}

func (r *ProductRepository) productFromIdempotencyData(data []byte) (*model.Product, error) {
	var rec idempotencyRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("idempotency unmarshal: %w", err)
	}
	return &rec.Product, nil
}

func (r *ProductRepository) storeIdempotency(ctx context.Context, idemKey, name string, priceCents int64, product model.Product) {
	rec := idempotencyRecord{Name: name, PriceCents: priceCents, Product: product}
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	if err := r.cache.Set(ctx, idemKey, data, r.idempotencyTTL).Err(); err != nil {
		log.Printf("idempotency set %s: %v", idemKey, err)
	}
}

func (r *ProductRepository) createWithIdempotencyLock(ctx context.Context, idemKey, name string, priceCents int64) (*model.Product, error) {
	lockKey := cache.LockKey(idemKey)
	lock, acquired, err := cache.TryAcquireLock(ctx, r.cache, lockKey, r.cacheLockTTL)
	if err != nil {
		log.Printf("idempotency lock acquire %s: %v", lockKey, err)
		return r.insertProduct(ctx, idemKey, name, priceCents)
	}

	if acquired {
		defer func() {
			if err := lock.Release(ctx); err != nil {
				log.Printf("idempotency lock release %s: %v", lockKey, err)
			}
		}()

		if p, err := r.resolveIdempotency(ctx, idemKey); err != nil || p != nil {
			return p, err
		}
		return r.insertProduct(ctx, idemKey, name, priceCents)
	}

	data, err := cache.WaitForCache(ctx, r.cache, idemKey, r.cacheLockMaxWait, r.cacheLockPoll)
	if err == nil {
		return r.productFromIdempotencyData(data)
	}

	if p, err := r.resolveIdempotency(ctx, idemKey); err != nil || p != nil {
		return p, err
	}

	return r.insertProduct(ctx, idemKey, name, priceCents)
}

func (r *ProductRepository) insertProduct(ctx context.Context, idemKey, name string, priceCents int64) (*model.Product, error) {
	p, err := r.insertProductRow(ctx, name, priceCents)
	if err != nil {
		return nil, err
	}
	r.storeIdempotency(ctx, idemKey, name, priceCents, *p)
	return p, nil
}
