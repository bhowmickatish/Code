package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/atish/go-cache-aside/internal/model"
	"github.com/atish/go-cache-aside/internal/repository"
)

type ProductHandler struct {
	repo              *repository.ProductRepository
	pageDefaultLimit  int
	pageDefaultOffset int
	pageMaxLimit      int
}

func NewProductHandler(repo *repository.ProductRepository, pageDefaultLimit, pageDefaultOffset, pageMaxLimit int) *ProductHandler {
	return &ProductHandler{
		repo:              repo,
		pageDefaultLimit:  pageDefaultLimit,
		pageDefaultOffset: pageDefaultOffset,
		pageMaxLimit:      pageMaxLimit,
	}
}

func (h *ProductHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/products", h.handleProducts)
	mux.HandleFunc("/products/", h.handleProductByID)
}

func (h *ProductHandler) handleProducts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		limit, offset, err := h.parsePagination(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		q := strings.TrimSpace(r.URL.Query().Get("q"))
		var page model.ProductPage
		if q != "" {
			page, err = h.repo.Search(r.Context(), q, limit, offset)
		} else {
			page, err = h.repo.List(r.Context(), limit, offset)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, page)

	case http.MethodPost:
		var body struct {
			Name  string  `json:"name"`
			Price float64 `json:"price"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if body.Name == "" || body.Price <= 0 {
			http.Error(w, "name and positive price are required", http.StatusBadRequest)
			return
		}

		p, err := h.repo.Create(r.Context(), body.Name, body.Price)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusCreated, p)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *ProductHandler) handleProductByID(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		p, err := h.repo.GetByID(r.Context(), id)
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				http.Error(w, repository.ErrNotFound.Error(), http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, p)

	case http.MethodPut:
		var body struct {
			Name  string  `json:"name"`
			Price float64 `json:"price"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if body.Name == "" || body.Price <= 0 {
			http.Error(w, "name and positive price are required", http.StatusBadRequest)
			return
		}

		p, err := h.repo.Update(r.Context(), id, body.Name, body.Price)
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				http.Error(w, repository.ErrNotFound.Error(), http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, p)

	case http.MethodDelete:
		if err := h.repo.Delete(r.Context(), id); err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				http.Error(w, repository.ErrNotFound.Error(), http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *ProductHandler) parsePagination(r *http.Request) (limit, offset int, err error) {
	limit = h.pageDefaultLimit
	offset = h.pageDefaultOffset

	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || limit < 1 {
			return 0, 0, fmt.Errorf("limit must be a positive integer")
		}
		if limit > h.pageMaxLimit {
			limit = h.pageMaxLimit
		}
	}

	if v := r.URL.Query().Get("offset"); v != "" {
		offset, err = strconv.Atoi(v)
		if err != nil || offset < 0 {
			return 0, 0, fmt.Errorf("offset must be a non-negative integer")
		}
	}

	return limit, offset, nil
}

func parseID(path string) (int64, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[0] != "products" {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseInt(parts[1], 10, 64)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
