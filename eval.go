// Copyright 2024 RunReveal Inc.
// SPDX-License-Identifier: Apache-2.0

package pql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/runreveal/pql/parser"
)

// Table represents an in-memory table.
type Table struct {
	Name    string
	Columns []string
	Data    [][]string
}

// Eval evaluates the given pql expression against the given tables.
func Eval(source string, tables []*Table) (*Table, error) {
	expr, err := parser.Parse(source)
	if err != nil {
		return nil, err
	}

	tableMap := make(map[string]*Table, len(tables))
	for _, tab := range tables {
		tableMap[tab.Name] = tab
	}
	return eval(expr, tableMap)
}

func eval(x *parser.TabularExpr, tables map[string]*Table) (*Table, error) {
	var curr *Table
	switch src := x.Source.(type) {
	case *parser.TableRef:
		curr = tables[src.Table.Name]
		if curr == nil {
			return nil, fmt.Errorf("unknown table %q", src.Table.Name)
		}
	default:
		return nil, fmt.Errorf("unhandled data source %T", src)
	}

	for _, op := range x.Operators {
		switch op := op.(type) {
		case *parser.CountOperator:
			curr = &Table{
				Columns: []string{"count()"},
				Data:    [][]string{{strconv.Itoa(len(curr.Data))}},
			}
		case *parser.TakeOperator:
			rowCount, err := evalExpr(op.RowCount, nil)
			if err != nil {
				return nil, err
			}
			n, err := strconv.Atoi(rowCount)
			if err != nil {
				return nil, err
			}
			if n < 0 {
				return nil, fmt.Errorf("negative row count")
			}
			curr = &Table{
				Columns: curr.Columns,
				Data:    curr.Data[:min(n, len(curr.Data))],
			}
		case *parser.WhereOperator:
			idents := map[string]string{
				// TODO(someday): These should only match if not quoted.
				"null":  "",
				"true":  "1",
				"false": "0",
			}
			newTable := &Table{
				Columns: curr.Columns,
				Data:    make([][]string, 0, len(curr.Data)),
			}
			for _, row := range curr.Data {
				// Fill in variables for current row.
				for i, val := range row {
					idents[curr.Columns[i]] = val
				}

				result, err := evalExpr(op.Predicate, idents)
				if err != nil {
					return nil, err
				}
				if stringToBool(result) {
					newTable.Data = append(newTable.Data, row)
				}
			}
			curr = newTable
		default:
			return nil, fmt.Errorf("unhandled operator %T", op)
		}
	}

	return curr, nil
}

func evalExpr(x parser.Expr, idents map[string]string) (string, error) {
	switch x := x.(type) {
	case *parser.ParenExpr:
		return evalExpr(x.X, idents)
	case *parser.BasicLit:
		return x.Value, nil
	case *parser.QualifiedIdent:
		if len(x.Parts) != 1 {
			return "", fmt.Errorf("qualified identifiers not supported")
		}
		name := x.Parts[0].Name
		value, ok := idents[name]
		if !ok {
			return "", fmt.Errorf("unrecognized identifier %q", name)
		}
		return value, nil
	case *parser.UnaryExpr:
		inner, err := evalExpr(x.X, idents)
		if err != nil {
			return "", err
		}

		switch x.Op {
		case parser.TokenPlus:
			return inner, nil
		case parser.TokenMinus:
			if pos, isNegative := strings.CutPrefix(inner, "-"); isNegative {
				return pos, nil
			} else {
				return "-" + inner, nil
			}
		default:
			return "", fmt.Errorf("unhandled unary operator %v", x.Op)
		}
	case *parser.BinaryExpr:
		a, err := evalExpr(x.X, idents)
		if err != nil {
			return "", err
		}

		// Short-circuit evaluation.
		switch x.Op {
		case parser.TokenAnd:
			if !stringToBool(a) {
				return a, nil
			}
		case parser.TokenOr:
			if stringToBool(a) {
				return a, nil
			}
		}

		b, err := evalExpr(x.Y, idents)
		if err != nil {
			return "", err
		}

		switch x.Op {
		case parser.TokenEq:
			return boolToString(a == b), nil
		case parser.TokenNE:
			return boolToString(a != b), nil
		case parser.TokenAnd, parser.TokenOr:
			return b, nil
		default:
			return "", fmt.Errorf("unhandled binary operator %v", x.Op)
		}
	case *parser.InExpr:
		a, err := evalExpr(x.X, idents)
		if err != nil {
			return "", err
		}

		for _, y := range x.Vals {
			b, err := evalExpr(y, idents)
			if err != nil {
				return "", err
			}
			if a == b {
				return boolToString(true), nil
			}
		}
		return boolToString(false), nil
	case *parser.CallExpr:
		f := evalFuncs[x.Func.Name]
		if f == nil {
			return "", fmt.Errorf("unknown function %s", x.Func.Name)
		}

		var args []string
		for _, a := range x.Args {
			aa, err := evalExpr(a, idents)
			if err != nil {
				return "", err
			}
			args = append(args, aa)
		}
		return f(args)
	default:
		return "", fmt.Errorf("unhandled expression %T", x)
	}
}

var evalFuncs = map[string]func(args []string) (string, error){
	"not": func(args []string) (string, error) {
		if len(args) != 1 {
			return "", fmt.Errorf("not(x) takes exactly one argument")
		}
		return boolToString(!stringToBool(args[0])), nil
	},
	"strcat": func(args []string) (string, error) {
		if len(args) == 0 {
			return "", fmt.Errorf("strcat(x, ...) takes at least one argument")
		}
		return strings.Join(args, ""), nil
	},
}

func stringToBool(s string) bool {
	return s != "" && s != "0"
}

func boolToString(b bool) string {
	if b {
		return "1"
	} else {
		return "0"
	}
}
