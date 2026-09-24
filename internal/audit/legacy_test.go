package audit

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// bannedIdentifiers are the reconcile-plan legacy names; any of them in a
// non-test source file violates the naming law.
var bannedIdentifiers = []string{
	"Produce", "ProduceBatch", "Publish", "Pop", "Stats", "TruncateQueue",
	"FetchOffset", "TaskOption", "FailOption", "WithScheduleAt", "WithVisibilityTimeout",
	"MaxSegmentBytes", "OriginalOffset", "DequeueNow",
	"OpProduce", "OpPublish", "OpPop", "OpStats", "OpTail", "OpFetchOffset",
	// metrics.md retirements: Metrics replaces Info, nothing invents Stats types.
	"Info", "EngineStats", "QueueStats", "InfoResult", "MetricsResult", "OpInfo",
	// split-era names: the old concrete types and manager verbs must not leak.
	"LogQueue", "TaskQueue", "TaskRef", "TaskID", "Message",
	"MIKYUTQ", "GetQueue", "CreateQueue", "DeleteQueue", "ListQueues",
}

// sdkSurfaceNames stay legal as Client interface methods, driver method names,
// selector references, and the SDK's own type aliases; they are banned as
// plain type or value references anywhere else.
var sdkSurfaceNames = map[string]bool{
	"LogQueue": true, "TaskQueue": true, "TaskRef": true, "TaskID": true,
	"DeleteQueue": true, "ListQueues": true,
}

var bannedOpPattern = regexp.MustCompile(`^OpTask[A-Za-z]*ByID$`)

func isBanned(name string) bool {
	if slices.Contains(bannedIdentifiers, name) {
		return true
	}
	return bannedOpPattern.MatchString(name)
}

// surfacePositions marks the identifier positions where a banned name is still
// the SDK surface itself: selector Sels (client.LogQueue, mikyu.TaskRef) and
// method declarations, including interface methods.
type surfacePositions struct {
	sel  map[token.Pos]bool
	decl map[token.Pos]bool
}

func collectSurfacePositions(f *ast.File) surfacePositions {
	s := surfacePositions{sel: map[token.Pos]bool{}, decl: map[token.Pos]bool{}}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.SelectorExpr:
			s.sel[n.Sel.Pos()] = true
		case *ast.FuncDecl:
			s.decl[n.Name.Pos()] = true
		case *ast.Field:
			if _, ok := n.Type.(*ast.FuncType); ok {
				for _, name := range n.Names {
					s.decl[name.Pos()] = true
				}
			}
		}
		return true
	})
	return s
}

// isSurfaceWhitelisted reports whether this occurrence of a banned name is the
// SDK surface: the root mikyu package, a selector reference, or a method name.
func isSurfaceWhitelisted(rel string, id *ast.Ident, s surfacePositions) bool {
	if !sdkSurfaceNames[id.Name] {
		return false
	}
	if filepath.ToSlash(filepath.Dir(rel)) == "." || filepath.ToSlash(filepath.Dir(rel)) == "internal/client" {
		return true
	}
	return s.sel[id.Pos()] || s.decl[id.Pos()]
}

// TestNoLegacyNames walks every non-test Go file in the module and fails on any
// banned identifier, wherever it is declared or referenced.
func TestNoLegacyNames(t *testing.T) {
	root, err := findModuleRoot()
	require.NoError(t, err)

	fset := token.NewFileSet()
	var hits []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch {
			case strings.HasPrefix(d.Name(), "."), d.Name() == "vendor", d.Name() == "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		surface := collectSurfacePositions(f)
		ast.Inspect(f, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok && isBanned(ident.Name) && !isSurfaceWhitelisted(rel, ident, surface) {
				pos := fset.Position(ident.Pos())
				hits = append(hits, rel+":"+strconv.Itoa(pos.Line)+":"+ident.Name)
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)

	slices.Sort(hits)
	require.Emptyf(t, hits, "legacy identifiers found in non-test sources:\n%s", strings.Join(hits, "\n"))
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}
