package audit

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInternalCommentStyle walks every non-test Go file under internal/ and
// cmd/: each package clause and every exported symbol carries at most one line
// of doc comment, or none. The mikyu facade sits at the repo root and is
// exempt, because it is the one package an end user reads.
func TestInternalCommentStyle(t *testing.T) {
	root, err := findModuleRoot()
	require.NoError(t, err)

	fset := token.NewFileSet()
	var violations []string
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
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
			f, perr := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
			if perr != nil {
				return perr
			}
			rel, _ := filepath.Rel(root, path)

			if lines := commentLines(f.Doc); lines > 1 {
				violations = append(violations, rel+": package clause has "+strconv.Itoa(lines)+" comment lines")
			}

			for _, decl := range f.Decls {
				switch d := decl.(type) {
				case *ast.FuncDecl:
					if d.Name.IsExported() {
						report(&violations, fset, rel, d.Doc, "func "+d.Name.Name)
					}
				case *ast.GenDecl:
					for _, spec := range d.Specs {
						switch s := spec.(type) {
						case *ast.TypeSpec:
							if s.Name.IsExported() {
								report(&violations, fset, rel, docOf(d, s.Doc), "type "+s.Name.Name)
							}
						case *ast.ValueSpec:
							for _, name := range s.Names {
								if name.IsExported() {
									report(&violations, fset, rel, docOf(d, s.Doc), "value "+name.Name)
								}
							}
						}
					}
				}
			}
			return nil
		})
		require.NoError(t, err)
	}

	slices.Sort(violations)
	require.Emptyf(t, violations, "comment-style violations:\n%s", strings.Join(violations, "\n"))
}

// docOf picks the comment a lone symbol inherits from its declaration block.
func docOf(decl *ast.GenDecl, spec *ast.CommentGroup) *ast.CommentGroup {
	if spec != nil {
		return spec
	}
	if len(decl.Specs) == 1 {
		return decl.Doc
	}
	return nil
}

func report(violations *[]string, fset *token.FileSet, rel string, doc *ast.CommentGroup, symbol string) {
	if lines := commentLines(doc); lines > 1 {
		pos := fset.Position(doc.Pos())
		*violations = append(*violations, rel+":"+strconv.Itoa(pos.Line)+": "+symbol+" has "+strconv.Itoa(lines)+" comment lines")
	}
}

// commentLines counts the lines of a doc comment, or zero when there is none.
func commentLines(doc *ast.CommentGroup) int {
	if doc == nil {
		return 0
	}
	return len(strings.Split(strings.TrimRight(doc.Text(), "\n"), "\n"))
}
