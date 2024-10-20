package main

import (
	"context"
	"database/sql"
	"os"

	_ "github.com/marcboeker/go-duckdb"

	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/agnivade/levenshtein"
)

func summarizeData(file string) (table.Writer, error) {

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return t, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	cmd := fmt.Sprintf(`
	SELECT column_name AS Var, column_type as Type, min as Min, max as Max, q50 as Median, approx_unique, count
	FROM (
			SUMMARIZE(
					SELECT * FROM "%s"
			)
	)`, file)
	rows, err := db.QueryContext(context.Background(), cmd)

	if err != nil {
		return t, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return t, fmt.Errorf("failed to get columns: %w", err)
	}

	var rowCount int64
	countCmd := fmt.Sprintf("SELECT COUNT(*) FROM '%s'", file)
	err = db.QueryRow(countCmd).Scan(&rowCount)
	if err != nil {
		return t, fmt.Errorf("failed to get row count: %w", err)
	}

	header := make(table.Row, len(columns))
	for i, col := range columns {
		header[i] = col
	}
	t.AppendHeader(header)

	var (
		colName      string
		colType      string
		min          string
		max          string
		q50          sql.NullString
		approxUnique float64
		count        float64
		medVal       string
		nCols        int64
	)
	for rows.Next() {
		nCols++
		err := rows.Scan(&colName, &colType, &min, &max, &q50, &approxUnique, &count)
		if q50.Valid {
			medVal = q50.String
		} else {
			medVal = ""
		}
		t.AppendRow([]interface{}{colName, colType, min, max, medVal, approxUnique, count})
		if err != nil {
			return t, fmt.Errorf("failed to scan row: %w", err)
		}

	}
	fmt.Printf("Rows, columns: %v, %v\n", rowCount, nCols)

	t.Render()
	if err = rows.Err(); err != nil {
		return t, fmt.Errorf("error during row iteration: %w", err)
	}

	return t, nil
}

func findFile(ptrn string) (string, error) {
	ext := filepath.Ext(ptrn)
	bestDist := int(^uint(0) >> 1) // Max int value
	bestString := ""

	walk := func(s string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(s) != ext {
			return nil
		}
		sDist := levenshtein.ComputeDistance(s, ptrn)
		if sDist < bestDist {
			bestDist = sDist
			bestString = s
		}
		return nil
	}
	err := filepath.WalkDir(".", walk)
	if bestString == "" {
		return "", fmt.Errorf("couldn find a match for %s", ptrn)
	}
	return bestString, err
}

func main() {
	file, err := findFile("df.parquet")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Found file: %s\n", file)
	_, err2 := summarizeData(file)
	if err2 != nil {
		panic(err2)
	}
}
