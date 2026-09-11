# gdoc-grid

A small Go CLI that fetches a **published Google Doc**, reads a coordinate table from its HTML, and prints the resulting **character grid** to stdout.

Useful for puzzle-style docs where characters are placed at `(x, y)` coordinates and the hidden message appears when the grid is rendered.

## Features

- Fetches a public/published Google Doc over HTTP
- Parses the first HTML table (`x`, character, `y` columns)
- Builds a sparse grid (empty cells are spaces)
- Prints one row per line to stdout
- Exported helpers (`PrintGridFromGoogleDoc`, `PrintGridFromGoogleDocTo`) for reuse in tests or other tools

## Prerequisites

- Go 1.26+
- Network access to the Google Doc URL
- The document must be **published to the web** (not a private edit link)

## Document format

The Google Doc must contain a table with **3 columns** and a header row:

| x-coordinate | Character | y-coordinate |
| --- | --- | --- |
| 0 | █ | 0 |
| 0 | ▀ | 1 |
| 1 | ▀ | 1 |

Rules enforced by the parser:

- Header row is skipped
- Each data row must have exactly 3 cells
- `x` and `y` must be non-negative integers
- Character cell must contain at least one rune (first rune is used)
- Duplicate coordinates overwrite earlier cells

## Quick start

```powershell
cd d:\Code\gdoc-grid
go run . "https://docs.google.com/document/d/e/.../pub"
```

Example output:

```
█▀
▀ 
```

## Build

```powershell
go build -o gdoc-grid.exe .
.\gdoc-grid.exe "https://docs.google.com/document/d/e/.../pub"
```

Dependencies are vendored (`vendor/`). Build with `-mod=vendor` if your environment requires it:

```powershell
go build -mod=vendor -o gdoc-grid.exe .
```

## Usage

```
gdoc-grid <google-doc-url>
```

| Argument | Description |
| --- | --- |
| `google-doc-url` | Published Google Doc URL (typically ends in `/pub`) |

Errors are written to stderr; exit code is `1` on failure.

## How it works

1. HTTP GET the document HTML (30s client timeout)
2. Parse tables with [goquery](https://github.com/PuerkitoBio/goquery)
3. For each data row, read `(x, char, y)` and place `char` in a 2D rune grid
4. Print rows `y = 0..maxY`, columns `x = 0..maxX`

## Programmatic use

`PrintGridFromGoogleDoc` and `PrintGridFromGoogleDocTo` are exported from `main.go` for tests or embedding. To reuse from another Go module, move them into an internal package (e.g. `internal/grid`).

## Dependencies

| Package | Use |
| --- | --- |
| `github.com/PuerkitoBio/goquery` | HTML table parsing |
