package mikyu_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestEveryExportHasDoc holds the public surface to its documentation
// contract: every exported declaration at the top level of the package,
// every exported interface method, and every exported struct field
// carries a doc comment. Test files are exempt.
func TestEveryExportHasDoc(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	pkg, ok := pkgs["mikyu"]
	if !ok {
		t.Fatal("package mikyu not found")
	}

	var missing []string
	report := func(pos token.Position, name string) {
		missing = append(missing, fmt.Sprintf("%s:%d %s", pos.Filename, pos.Line, name))
	}

	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if !d.Name.IsExported() || d.Doc != nil {
					continue
				}
				if d.Recv != nil {
					if base := receiverType(d); base != "" && !ast.IsExported(base) {
						continue
					}
				}
				report(fset.Position(d.Pos()), d.Name.Name)
			case *ast.GenDecl:
				switch d.Tok {
				case token.TYPE:
					for _, spec := range d.Specs {
						ts, ok := spec.(*ast.TypeSpec)
						if !ok || !ts.Name.IsExported() {
							continue
						}
						if ts.Doc == nil && d.Doc == nil {
							report(fset.Position(ts.Pos()), ts.Name.Name)
						}
						if st, ok := ts.Type.(*ast.StructType); ok {
							for _, field := range st.Fields.List {
								for _, name := range field.Names {
									if name.IsExported() && field.Doc == nil {
										report(fset.Position(name.Pos()), ts.Name.Name+"."+name.Name)
									}
								}
							}
						}
					}
				case token.CONST, token.VAR:
					for _, spec := range d.Specs {
						vs, ok := spec.(*ast.ValueSpec)
						if !ok {
							continue
						}
						for _, name := range vs.Names {
							if name.IsExported() && d.Doc == nil && vs.Doc == nil {
								report(fset.Position(name.Pos()), name.Name)
							}
						}
					}
				}
			}
		}
	}

	if len(missing) > 0 {
		t.Errorf("%d exported names are missing doc comments:\n%s", len(missing), strings.Join(missing, "\n"))
	}
}

// receiverType returns the base name of a method's receiver type, or ""
// when the receiver is malformed. Pointer receivers report the pointed-to
// type.
func receiverType(d *ast.FuncDecl) string {
	switch expr := d.Recv.List[0].Type.(type) {
	case *ast.Ident:
		return expr.Name
	case *ast.StarExpr:
		if id, ok := expr.X.(*ast.Ident); ok {
			return id.Name
		}
	}
	return ""
}
