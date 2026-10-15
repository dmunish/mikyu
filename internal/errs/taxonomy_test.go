package errs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// declaredSentinelValues is every sentinel the taxonomy is expected to cover.
func declaredSentinelValues() map[string]error {
	return map[string]error{
		"engine.ErrQueueExists":     engine.ErrQueueExists,
		"engine.ErrQueueNotFound":   engine.ErrQueueNotFound,
		"engine.ErrOffsetNotFound":  engine.ErrOffsetNotFound,
		"engine.ErrMessageTooLarge": engine.ErrMessageTooLarge,
		"engine.ErrRecordTooLarge":  engine.ErrRecordTooLarge,
		"engine.ErrOffsetTruncated": engine.ErrOffsetTruncated,
		"engine.ErrQueueClosed":     engine.ErrQueueClosed,
		"engine.ErrNoData":          engine.ErrNoData,
		"engine.ErrRecordCompacted": engine.ErrRecordCompacted,
		"engine.ErrDiskFull":        engine.ErrDiskFull,
		"engine.ErrCorruptRecord":   engine.ErrCorruptRecord,
		"engine.ErrEncryptionKey":   engine.ErrEncryptionKey,

		"spec.ErrInvalidConfig":  spec.ErrInvalidConfig,
		"spec.ErrImmutableField": spec.ErrImmutableField,
		"spec.ErrSpecTooLarge":   spec.ErrSpecTooLarge,
		"spec.ErrSpecTooNew":     spec.ErrSpecTooNew,

		"manager.ErrWrongKind": manager.ErrWrongKind,

		"node.ErrRegistryCorrupt":         node.ErrRegistryCorrupt,
		"node.ErrLocked":                  node.ErrLocked,
		"node.ErrSessionDefaultsMismatch": node.ErrSessionDefaultsMismatch,

		"taskqueue.ErrTaskNotAvailable":   taskqueue.ErrTaskNotAvailable,
		"taskqueue.ErrTaskNotFound":       taskqueue.ErrTaskNotFound,
		"taskqueue.ErrTaskNotInflight":    taskqueue.ErrTaskNotInflight,
		"taskqueue.ErrTaskStale":          taskqueue.ErrTaskStale,
		"taskqueue.ErrTaskClosed":         taskqueue.ErrTaskClosed,
		"taskqueue.ErrInvalidTask":        taskqueue.ErrInvalidTask,
		"taskqueue.ErrCheckpointCorrupt":  taskqueue.ErrCheckpointCorrupt,
		"taskqueue.ErrDeadLetterNotFound": taskqueue.ErrDeadLetterNotFound,
		"taskqueue.ErrDeadLetterStale":    taskqueue.ErrDeadLetterStale,
		"taskqueue.ErrQueueFull":          taskqueue.ErrQueueFull,
		"taskqueue.ErrArchiveFailed":      taskqueue.ErrArchiveFailed,

		"protocol.ErrTooManyConnections": protocol.ErrTooManyConnections,
		"protocol.ErrUnauthorized":       protocol.ErrUnauthorized,
		"protocol.ErrTruncatedBody":      protocol.ErrTruncatedBody,
		"protocol.ErrInvalidTask":        protocol.ErrInvalidTask,
	}
}

// TestErrorTaxonomyTotal proves every declared sentinel has a wire code and every wire code has a sentinel.
func TestErrorTaxonomyTotal(t *testing.T) {
	root, err := findModuleRoot()
	require.NoError(t, err)

	all := declaredSentinelValues()
	declared := map[string]bool{}
	for _, pkg := range []struct{ dir, name string }{
		{"internal/engine", "engine"},
		{"internal/spec", "spec"},
		{"internal/manager", "manager"},
		{"internal/node", "node"},
		{"internal/queue/taskqueue", "taskqueue"},
		{"internal/transport/protocol", "protocol"},
	} {
		for _, name := range sentinelNames(t, filepath.Join(root, pkg.dir), pkg.name) {
			declared[name] = true
		}
	}
	require.NotEmpty(t, declared)

	// Forward: every sentinel declared in source is mapped or deliberately off-wire.
	for name := range declared {
		sentinel, ok := all[name]
		require.Truef(t, ok, "sentinel %s is declared but absent from the taxonomy test's value map", name)
		if reason, offWire := NonWireSentinels[sentinel]; offWire {
			assert.NotEmpty(t, reason)
			continue
		}
		_, ok = CodeFor(sentinel)
		assert.Truef(t, ok, "sentinel %s has no wire code", name)
	}
	// The test's value map must not invent sentinels.
	for name := range all {
		assert.Truef(t, declared[name], "taxonomy test lists %s but no error package declares it", name)
	}

	// Every table entry resolves to its own code.
	for _, e := range Table {
		code, ok := CodeFor(e.Sentinel)
		require.Truef(t, ok, "CodeFor(%v) failed", e.Sentinel)
		assert.Equalf(t, e.Code, code, "CodeFor(%v)", e.Sentinel)
	}

	// Reverse: every protocol code except the unknown catch-all has a sentinel, and no table code is orphaned.
	codes := protocolCodes(t, filepath.Join(root, "internal/transport/protocol/errors.go"))
	require.NotEmpty(t, codes)
	for code, name := range codes {
		if code == protocol.ErrCodeUnknown {
			continue
		}
		_, ok := SentinelFor(code)
		assert.Truef(t, ok, "wire code %s (0x%02X) has no sentinel", name, code)
	}
	for _, e := range Table {
		_, ok := codes[e.Code]
		assert.Truef(t, ok, "taxonomy code 0x%02X is not declared in protocol/errors.go", e.Code)
	}
}

// sentinelNames lists package-qualified Err* variables declared with errors.New in a package's sources.
func sentinelNames(t *testing.T, pkgDir, pkgName string) []string {
	t.Helper()
	entries, err := os.ReadDir(pkgDir)
	require.NoError(t, err)

	fset := token.NewFileSet()
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(pkgDir, e.Name()), nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		ast.Inspect(f, func(n ast.Node) bool {
			vs, ok := n.(*ast.ValueSpec)
			if !ok {
				return true
			}
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, "Err") || i >= len(vs.Values) {
					continue
				}
				if isErrorsNew(vs.Values[i]) {
					names = append(names, pkgName+"."+name.Name)
				}
			}
			return true
		})
	}
	return names
}

// isErrorsNew reports whether an expression is a call to errors.New.
func isErrorsNew(e ast.Expr) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == "errors" && sel.Sel.Name == "New"
}

// protocolCodes maps each declared ErrCode* constant to its byte value.
func protocolCodes(t *testing.T, path string) map[byte]string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	require.NoError(t, err)

	out := map[byte]string{}
	ast.Inspect(f, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range vs.Names {
			if !strings.HasPrefix(name.Name, "ErrCode") || i >= len(vs.Values) {
				continue
			}
			lit, ok := vs.Values[i].(*ast.BasicLit)
			if !ok || lit.Kind != token.INT {
				continue
			}
			v, err := strconv.ParseUint(lit.Value, 0, 8)
			if err != nil {
				continue
			}
			out[byte(v)] = name.Name
		}
		return true
	})
	return out
}

// findModuleRoot walks up from the working directory to the module root.
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