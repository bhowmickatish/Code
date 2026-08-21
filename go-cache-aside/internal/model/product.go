package model

import "time"

type Product struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	PriceCents int64     `json:"price_cents"`
	CreatedAt  time.Time `json:"created_at"`
}

type ProductPage struct {
	Items  []Product `json:"items"`
	Total  int       `json:"total"`
	Limit  int       `json:"limit"`
	Offset int       `json:"offset"`
}
