package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

var httpClient = &http.Client{Timeout: 30 * time.Second}

type gridCell struct {
	x  int
	y  int
	ch rune
}

// PrintGridFromGoogleDoc fetches the published Google Doc at url, parses its
// coordinate table, and prints the resulting character grid to stdout.
func PrintGridFromGoogleDoc(url string) error {
	return PrintGridFromGoogleDocTo(os.Stdout, url)
}

// PrintGridFromGoogleDocTo is like PrintGridFromGoogleDoc but writes output to w.
func PrintGridFromGoogleDocTo(w io.Writer, url string) error {
	cells, err := fetchGridCells(url)
	if err != nil {
		return err
	}

	grid, maxY := buildGrid(cells)
	printGrid(w, grid, maxY)
	return nil
}

func fetchGridCells(url string) ([]gridCell, error) {
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch document: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("fetch document: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse document HTML: %w", err)
	}

	return parseGridCells(doc)
}

func parseGridCells(doc *goquery.Document) ([]gridCell, error) {
	var cells []gridCell
	var parseErr error

	doc.Find("table tr").Each(func(rowIndex int, row *goquery.Selection) {
		if parseErr != nil {
			return
		}
		if rowIndex == 0 {
			return
		}

		columns := row.Find("td")
		if columns.Length() != 3 {
			parseErr = fmt.Errorf("row %d: expected 3 columns, got %d", rowIndex+1, columns.Length())
			return
		}

		x, err := strconv.Atoi(strings.TrimSpace(columns.Eq(0).Text()))
		if err != nil {
			parseErr = fmt.Errorf("row %d: invalid x coordinate: %w", rowIndex+1, err)
			return
		}
		if x < 0 {
			parseErr = fmt.Errorf("row %d: x coordinate must be non-negative, got %d", rowIndex+1, x)
			return
		}

		charText := strings.TrimSpace(columns.Eq(1).Text())
		if charText == "" {
			parseErr = fmt.Errorf("row %d: empty character", rowIndex+1)
			return
		}

		y, err := strconv.Atoi(strings.TrimSpace(columns.Eq(2).Text()))
		if err != nil {
			parseErr = fmt.Errorf("row %d: invalid y coordinate: %w", rowIndex+1, err)
			return
		}
		if y < 0 {
			parseErr = fmt.Errorf("row %d: y coordinate must be non-negative, got %d", rowIndex+1, y)
			return
		}

		cells = append(cells, gridCell{x: x, y: y, ch: []rune(charText)[0]})
	})

	if parseErr != nil {
		return nil, parseErr
	}
	if len(cells) == 0 {
		return nil, fmt.Errorf("no grid cells found in document table")
	}

	return cells, nil
}

func buildGrid(cells []gridCell) ([][]rune, int) {
	maxX, maxY := 0, 0
	for _, cell := range cells {
		if cell.x > maxX {
			maxX = cell.x
		}
		if cell.y > maxY {
			maxY = cell.y
		}
	}

	grid := make([][]rune, maxY+1)
	for y := 0; y <= maxY; y++ {
		grid[y] = make([]rune, maxX+1)
		for x := 0; x <= maxX; x++ {
			grid[y][x] = ' '
		}
	}

	for _, cell := range cells {
		grid[cell.y][cell.x] = cell.ch
	}

	return grid, maxY
}

func printGrid(w io.Writer, grid [][]rune, maxY int) {
	for y := 0; y <= maxY; y++ {
		fmt.Fprintln(w, string(grid[y]))
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <google-doc-url>\n", os.Args[0])
		os.Exit(1)
	}

	if err := PrintGridFromGoogleDoc(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
