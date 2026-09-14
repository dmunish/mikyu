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
	"MaxSegmentAge", "MaxSegmentBytes", "OriginalOffset", "DequeueNow",
	"OpProduce", "OpPublish", "OpPop", "OpStats", "OpTail", "OpFetchOffset",
	// metrics.md retirements: Metrics replaces Info, nothing invents Stats types.
	"Info", "EngineStats", "QueueStats", "InfoResult", "MetricsResult", "OpInfo",
}

var bannedOpPattern = regexp.MustCompile(`^OpTask[A-Za-z]*ByID$`)

func isBanned(name string) bool {
	if slices.Contains(bannedIdentifiers, name) {
		return true
	}
	return bannedOpPattern.MatchString(name)
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
		ast.Inspect(f, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok && isBanned(ident.Name) {
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
