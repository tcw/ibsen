package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// eventLevels are the zerolog/log functions that start an event. An event does nothing
// until it is sent: log.Fatal().Err(err) alone neither logs nor exits.
var eventLevels = map[string]bool{
	"Trace": true, "Debug": true, "Info": true, "Warn": true, "Error": true, "Err": true,
	"Fatal": true, "Panic": true, "WithLevel": true, "Log": true,
}

var eventSenders = map[string]bool{"Msg": true, "Msgf": true, "MsgFunc": true, "Send": true}

// TestZerologEventsAreSent fails for any statement in production code that builds a zerolog
// event and drops it without sending it.
func TestZerologEventsAreSent(t *testing.T) {
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != "." && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		logName := zerologImportName(file)
		if logName == "" {
			return nil
		}
		ast.Inspect(file, func(n ast.Node) bool {
			stmt, ok := n.(*ast.ExprStmt)
			if !ok {
				return true
			}
			call, ok := stmt.X.(*ast.CallExpr)
			if !ok {
				return true
			}
			if method, isEvent := eventChain(call, logName); isEvent && !eventSenders[method] {
				t.Errorf("%s: zerolog event is never sent (ends in .%s, add .Msg or .Send)", fset.Position(stmt.Pos()), method)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// zerologImportName returns the name the file uses for github.com/rs/zerolog/log, or "".
func zerologImportName(file *ast.File) string {
	for _, imp := range file.Imports {
		if path, _ := strconv.Unquote(imp.Path.Value); path == "github.com/rs/zerolog/log" {
			if imp.Name != nil {
				return imp.Name.Name
			}
			return "log"
		}
	}
	return ""
}

// eventChain reports whether call is a method chain that starts with a zerolog event, such
// as log.Warn().Str(...).Msg(...), and returns the name of the outermost method.
func eventChain(call *ast.CallExpr, logName string) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	outer := sel.Sel.Name
	for {
		if pkg, ok := sel.X.(*ast.Ident); ok {
			return outer, pkg.Name == logName && eventLevels[sel.Sel.Name]
		}
		inner, ok := sel.X.(*ast.CallExpr)
		if !ok {
			return "", false
		}
		if sel, ok = inner.Fun.(*ast.SelectorExpr); !ok {
			return "", false
		}
	}
}
