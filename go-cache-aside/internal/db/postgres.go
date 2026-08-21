package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS products (
			id          BIGSERIAL PRIMARY KEY,
			name        TEXT NOT NULL,
			price_cents BIGINT NOT NULL,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		DO $$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_name = 'products' AND column_name = 'price'
			) THEN
				ALTER TABLE products ADD COLUMN IF NOT EXISTS price_cents BIGINT;
				UPDATE products
				SET price_cents = ROUND(price * 100)::BIGINT
				WHERE price_cents IS NULL;
				ALTER TABLE products DROP COLUMN price;
				ALTER TABLE products ALTER COLUMN price_cents SET NOT NULL;
			END IF;
		END $$;
	`)
	return err
}
