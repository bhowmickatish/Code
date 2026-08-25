package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/atish/go-cache-aside/internal/cache"
	"github.com/atish/go-cache-aside/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const productKeyPrefix = "product:"

type ProductRepository struct {
	db                *pgxpool.Pool
	cache             redis.UniversalClient
	cacheTTL          time.Duration
	idempotencyTTL    time.Duration
	cacheLockTTL      time.Duration
	cacheLockMaxWait  time.Duration
	cacheLockPoll     time.Duration
	loadGroup         singleflight.Group
}

func NewProductRepository(
	db *pgxpool.Pool,
	cache redis.UniversalClient,
	cacheTTL, idempotencyTTL, cacheLockTTL, cacheLockMaxWait, cacheLockPoll time.Duration,
) *ProductRepository {
	return &ProductRepository{
		db:               db,
		cache:            cache,
		cacheTTL:         cacheTTL,
		idempotencyTTL:   idempotencyTTL,
		cacheLockTTL:     cacheLockTTL,
		cacheLockMaxWait: cacheLockMaxWait,
		cacheLockPoll:    cacheLockPoll,
	}
}

func (r *ProductRepository) GetByID(ctx context.Context, id int64) (*model.Product, error) {
	key := productKey(id)

	p, ok, err := r.getFromCache(ctx, key)
	if err != nil {
		return nil, err
	}
	if ok {
		return p, nil
	}

	flightKey := strconv.FormatInt(id, 10)
	ch := r.loadGroup.DoChan(flightKey, func() (any, error) {
		loadCtx, cancel := context.WithTimeout(context.Background(), r.cacheLockMaxWait+r.cacheLockTTL)
		defer cancel()
		return r.loadThroughGates(loadCtx, id, key)
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		p, ok := res.Val.(*model.Product)
		if !ok {
			return nil, fmt.Errorf("unexpected load result for product %d", id)
		}
		return p, nil
	}
}

func (r *ProductRepository) loadThroughGates(ctx context.Context, id int64, cacheKey string) (*model.Product, error) {
	if p, ok, err := r.getFromCache(ctx, cacheKey); err != nil {
		return nil, err
	} else if ok {
		return p, nil
	}

	lockKey := cache.LockKey(cacheKey)
	lock, acquired, err := cache.TryAcquireLock(ctx, r.cache, lockKey, r.cacheLockTTL)
	if err != nil {
		log.Printf("cache lock acquire %s: %v", lockKey, err)
		return r.loadAndCache(ctx, id, cacheKey)
	}

	if acquired {
		defer func() {
			if err := lock.Release(ctx); err != nil {
				log.Printf("cache lock release %s: %v", lockKey, err)
			}
		}()

		if p, ok, err := r.getFromCache(ctx, cacheKey); err != nil {
			return nil, err
		} else if ok {
			return p, nil
		}

		return r.loadAndCache(ctx, id, cacheKey)
	}

	data, err := cache.WaitUntilCached(ctx, r.cache, cacheKey, r.cacheLockMaxWait, r.cacheLockPoll)
	if err == nil {
		return r.unmarshalProduct(data)
	}
	if !errors.Is(err, cache.ErrWaitTimeout) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		log.Printf("cache wait %s: %v", cacheKey, err)
	}

	if p, ok, err := r.getFromCache(ctx, cacheKey); err != nil {
		return nil, err
	} else if ok {
		return p, nil
	}

	return r.loadAndCache(ctx, id, cacheKey)
}

func (r *ProductRepository) getFromCache(ctx context.Context, key string) (*model.Product, bool, error) {
	cached, err := r.cache.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("cache get: %w", err)
	}
	p, err := r.unmarshalProduct(cached)
	if err != nil {
		return nil, false, nil
	}
	return p, true, nil
}

func (r *ProductRepository) unmarshalProduct(data []byte) (*model.Product, error) {
	var p model.Product
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("cache unmarshal: %w", err)
	}
	return &p, nil
}

func (r *ProductRepository) loadAndCache(ctx context.Context, id int64, key string) (*model.Product, error) {
	p, err := r.getFromDB(ctx, id)
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(p)
	if err != nil {
		return p, nil
	}
	if err := r.cache.Set(ctx, key, data, r.cacheTTL).Err(); err != nil {
		log.Printf("cache set %s: %v", key, err)
	}

	return p, nil
}

func (r *ProductRepository) Create(ctx context.Context, name string, priceCents int64) (*model.Product, error) {
	idemKey := idempotencyFingerprint(name, priceCents)

	if p, err := r.resolveIdempotency(ctx, idemKey); err != nil || p != nil {
		return p, err
	}

	return r.createWithIdempotencyLock(ctx, idemKey, name, priceCents)
}

func (r *ProductRepository) insertProductRow(ctx context.Context, name string, priceCents int64) (*model.Product, error) {
	var p model.Product
	err := r.db.QueryRow(ctx,
		`INSERT INTO products (name, price_cents) VALUES ($1, $2)
		 RETURNING id, name, price_cents, created_at`,
		name, priceCents,
	).Scan(&p.ID, &p.Name, &p.PriceCents, &p.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert product: %w", err)
	}
	return &p, nil
}

func (r *ProductRepository) Update(ctx context.Context, id int64, name string, priceCents int64) (*model.Product, error) {
	var p model.Product
	err := r.db.QueryRow(ctx,
		`UPDATE products SET name = $2, price_cents = $3 WHERE id = $1
		 RETURNING id, name, price_cents, created_at`,
		id, name, priceCents,
	).Scan(&p.ID, &p.Name, &p.PriceCents, &p.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("update product: %w", err)
	}

	if err := r.cache.Del(ctx, productKey(id)).Err(); err != nil {
		log.Printf("cache invalidate %s: %v", productKey(id), err)
	}

	return &p, nil
}

func (r *ProductRepository) Delete(ctx context.Context, id int64) error {
	tag, err := r.db.Exec(ctx, `DELETE FROM products WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete product: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	if err := r.cache.Del(ctx, productKey(id)).Err(); err != nil {
		log.Printf("cache invalidate %s: %v", productKey(id), err)
	}
	return nil
}

// List returns a paginated slice of products directly from Postgres (no cache).
func (r *ProductRepository) List(ctx context.Context, limit, offset int) (model.ProductPage, error) {
	var total int
	if err := r.db.QueryRow(ctx, `SELECT COUNT(*) FROM products`).Scan(&total); err != nil {
		return model.ProductPage{}, fmt.Errorf("count products: %w", err)
	}

	rows, err := r.db.Query(ctx,
		`SELECT id, name, price_cents, created_at FROM products ORDER BY id LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return model.ProductPage{}, fmt.Errorf("list products: %w", err)
	}
	defer rows.Close()

	items, err := scanProducts(rows)
	if err != nil {
		return model.ProductPage{}, err
	}
	return model.ProductPage{Items: items, Total: total, Limit: limit, Offset: offset}, nil
}

// Search finds products by name substring directly from Postgres (no cache).
func (r *ProductRepository) Search(ctx context.Context, query string, limit, offset int) (model.ProductPage, error) {
	var total int
	if err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM products WHERE name ILIKE '%' || $1 || '%'`,
		query,
	).Scan(&total); err != nil {
		return model.ProductPage{}, fmt.Errorf("count products: %w", err)
	}

	rows, err := r.db.Query(ctx,
		`SELECT id, name, price_cents, created_at FROM products
		 WHERE name ILIKE '%' || $1 || '%' ORDER BY id LIMIT $2 OFFSET $3`,
		query, limit, offset,
	)
	if err != nil {
		return model.ProductPage{}, fmt.Errorf("search products: %w", err)
	}
	defer rows.Close()

	items, err := scanProducts(rows)
	if err != nil {
		return model.ProductPage{}, err
	}
	return model.ProductPage{Items: items, Total: total, Limit: limit, Offset: offset}, nil
}

func scanProducts(rows pgx.Rows) ([]model.Product, error) {
	products := make([]model.Product, 0)
	for rows.Next() {
		var p model.Product
		if err := rows.Scan(&p.ID, &p.Name, &p.PriceCents, &p.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan product: %w", err)
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read products: %w", err)
	}
	return products, nil
}

func (r *ProductRepository) getFromDB(ctx context.Context, id int64) (*model.Product, error) {
	var p model.Product
	err := r.db.QueryRow(ctx,
		`SELECT id, name, price_cents, created_at FROM products WHERE id = $1`, id,
	).Scan(&p.ID, &p.Name, &p.PriceCents, &p.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query product: %w", err)
	}
	return &p, nil
}

func productKey(id int64) string {
	return fmt.Sprintf("%s%d", productKeyPrefix, id)
}
