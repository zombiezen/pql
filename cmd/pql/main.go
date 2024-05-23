// Copyright 2024 RunReveal Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/runreveal/pql"
	"github.com/runreveal/pql/parser"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"zombiezen.com/go/bass/sigterm"
)

func main() {
	rootCommand := &cobra.Command{
		Use:   "pql [options] [FILE [...]]",
		Short: "Translate Pipeline Query Language into SQL",

		DisableFlagsInUseLine: true,
		SilenceErrors:         true,
		SilenceUsage:          true,
	}
	outputPath := rootCommand.Flags().StringP("output", "o", "", "file to write SQL to (defaults to stdout)")
	rootCommand.RunE = func(cmd *cobra.Command, args []string) (err error) {
		input, err := makeInput(args)
		if err != nil {
			return err
		}
		output, err := makeOutput(*outputPath)
		if err != nil {
			input.Close()
			return err
		}

		err = run(cmd.Context(), output, input, func(err error) {
			fmt.Fprintf(os.Stderr, "pql: %v\n", err)
		})
		if err2 := output.Close(); err == nil {
			err = err2
		}
		input.Close()
		return err
	}

	{
		c := &cobra.Command{
			Use:   "eval [--table FILE [...]] [FILE [...]]",
			Short: "Evaluate Pipeline Query Language",

			DisableFlagsInUseLine: true,
			SilenceErrors:         true,
			SilenceUsage:          true,
		}
		tables := c.Flags().StringArray("table", nil, "`path` to ")
		outputPath := c.Flags().StringP("output", "o", "", "`file` to write CSV to (defaults to stdout)")
		c.RunE = func(cmd *cobra.Command, args []string) (err error) {
			input, err := makeInput(args)
			if err != nil {
				return err
			}
			output, err := makeOutput(*outputPath)
			if err != nil {
				input.Close()
				return err
			}

			err = runEval(cmd.Context(), *tables, output, input, func(err error) {
				fmt.Fprintf(os.Stderr, "pql: %v\n", err)
			})
			if err2 := output.Close(); err == nil {
				err = err2
			}
			input.Close()
			return err
		}
		rootCommand.AddCommand(c)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), sigterm.Signals()...)
	err := rootCommand.ExecuteContext(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "pql: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, output io.Writer, input io.Reader, logError func(error)) error {
	scanner := bufio.NewScanner(input)
	sb := new(strings.Builder)

	if isTerminal(input) {
		// Nudge for usage if running interactively.
		fmt.Fprintln(os.Stderr, "Reading from terminal (use semicolons to end statements)...")
	}

	var finalError error
	for scanner.Scan() {
		sb.Write(scanner.Bytes())
		sb.WriteByte('\n')

		statements := parser.SplitStatements(sb.String())
		if len(statements) == 1 {
			continue
		}

		for _, stmt := range statements[:len(statements)-1] {
			sql, err := pql.Compile(stmt)
			if err != nil {
				logError(err)
				finalError = errors.New("one or more statements could not be compiled")
				continue
			}
			fmt.Fprintf(output, "%s\n\n", sql)
		}

		sb.Reset()
		sb.WriteString(statements[len(statements)-1])
	}

	if stmt := sb.String(); len(parser.Scan(stmt)) > 0 {
		sql, err := pql.Compile(stmt)
		if err != nil {
			logError(err)
			return errors.New("one or more statements could not be compiled")
		}
		fmt.Fprintf(output, "%s\n\n", sql)
	}

	return finalError
}

func runEval(ctx context.Context, tablePaths []string, output io.Writer, source io.Reader, logError func(error)) error {
	scanner := bufio.NewScanner(source)
	sb := new(strings.Builder)

	if isTerminal(source) {
		// Nudge for usage if running interactively.
		fmt.Fprintln(os.Stderr, "Reading from terminal (use semicolons to end statements)...")
	}

	var tables []*pql.Table
	for _, path := range tablePaths {
		tab, err := readTable(path)
		if err != nil {
			return err
		}
		tables = append(tables, tab)
	}

	var finalError error
	w := csv.NewWriter(output)
	defer w.Flush()
	for scanner.Scan() {
		sb.Write(scanner.Bytes())
		sb.WriteByte('\n')

		statements := parser.SplitStatements(sb.String())
		if len(statements) == 1 {
			continue
		}

		for _, stmt := range statements[:len(statements)-1] {
			result, err := pql.Eval(stmt, tables)
			if err != nil {
				logError(err)
				finalError = errors.New("one or more statements could not be compiled")
				continue
			}
			w.Write(result.Columns)
			w.WriteAll(result.Data)
		}

		sb.Reset()
		sb.WriteString(statements[len(statements)-1])
	}

	if stmt := sb.String(); len(parser.Scan(stmt)) > 0 {
		result, err := pql.Eval(stmt, tables)
		if err != nil {
			logError(err)
			return errors.New("one or more statements could not be compiled")
		}
		w.Write(result.Columns)
		w.WriteAll(result.Data)
	}

	return finalError
}

func readTable(path string) (*pql.Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	tab := &pql.Table{
		Name: strings.TrimSuffix(filepath.Base(path), ".csv"),
	}
	r := csv.NewReader(f)
	tab.Columns, err = r.Read()
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("read %s: %v", path, err)
	}
	for {
		row, err := r.Read()
		if err == io.EOF {
			return tab, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %v", path, err)
		}
		tab.Data = append(tab.Data, row)
	}
}

func makeInput(args []string) (io.ReadCloser, error) {
	if len(args) == 0 || len(args) == 1 && args[0] == "-" {
		return nopReadCloser{os.Stdin}, nil
	}
	if len(args) == 1 {
		return os.Open(args[0])
	}

	readers := make([]io.ReadCloser, 0, len(args))
	for _, path := range args {
		if path == "-" {
			readers = append(readers, nopReadCloser{os.Stdin})
			continue
		}

		f, err := os.Open(path)
		if err != nil {
			for _, c := range readers {
				c.Close()
			}
			return nil, err
		}
		readers = append(readers, f)
	}
	return &multiReadCloser{readers}, nil
}

func makeOutput(arg string) (io.WriteCloser, error) {
	if arg == "" || arg == "-" {
		return nopWriteCloser{os.Stdout}, nil
	}
	return os.Create(arg)
}

func isTerminal(r io.Reader) bool {
	for {
		switch rt := r.(type) {
		case *os.File:
			return term.IsTerminal(int(rt.Fd()))
		case nopReadCloser:
			r = rt.Reader
		default:
			return false
		}
	}
}

// A multiReadCloser is a logical concatenation of its input readers,
// much like [io.MultiReader].
// However, it also implements [io.Closer]
// and closes its inputs as they are finished reading.
type multiReadCloser struct {
	readers []io.ReadCloser
}

func (mrc *multiReadCloser) Read(p []byte) (n int, err error) {
	for len(mrc.readers) > 0 {
		n, err = mrc.readers[0].Read(p)
		if err == io.EOF {
			mrc.readers[0].Close()
			mrc.readers[0] = nil
			mrc.readers = mrc.readers[1:]
		}
		if n > 0 || err != io.EOF {
			if err == io.EOF && len(mrc.readers) > 0 {
				err = nil
			}
			return
		}
	}
	return 0, io.EOF
}

func (mrc *multiReadCloser) Close() error {
	var firstError error
	for _, rc := range mrc.readers {
		if err := rc.Close(); firstError == nil {
			firstError = err
		}
	}
	mrc.readers = nil
	return firstError
}

type nopReadCloser struct {
	io.Reader
}

func (nopReadCloser) Close() error { return nil }

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }
