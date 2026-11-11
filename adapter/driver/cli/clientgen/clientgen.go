// Package clientgen hands out ready-to-build Ibsen clients for other languages, generated from
// api/grpcApi/ibsen.proto when Ibsen was built and embedded in the binary, so a user needs no
// protoc and no plugins: `ibsen generate client java` writes a folder that builds.
//
// The folders under clients/ are copied out as they are, with two adjustments: a file named
// *.tmpl loses the suffix (Go sources and go.mod are kept that way so the main module does not
// build them), and the tokens @@MODULE@@, @@PACKAGE@@, @@PACKAGE_PATH@@, @@IBSEN_VERSION@@ and
// @@PROTO_VERSION@@ are filled in, in file contents and in names. Nothing is generated at run
// time. scripts/gen-clients.sh regenerates the stubs; CI fails if they are stale.
//
// It is part of the CLI and nothing else: the core and the embedded root link none of it.
package clientgen

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

//go:embed all:clients ibsen.proto
var bundle embed.FS

// Language is one bundled client.
type Language struct {
	Name string
	// DefaultPackage is what --package means when it is not given: a Go module path, a Java
	// package, a Python package or an npm package name.
	DefaultPackage string
	// Builds says what building the folder takes.
	Builds string
}

var languages = map[string]Language{
	"go":         {Name: "go", DefaultPackage: "example.com/ibsen-client", Builds: "Go 1.25"},
	"java":       {Name: "java", DefaultPackage: "com.example.ibsen", Builds: "Java 17 and Maven"},
	"python":     {Name: "python", DefaultPackage: "ibsen_client", Builds: "Python 3.9"},
	"typescript": {Name: "typescript", DefaultPackage: "ibsen-client", Builds: "Node 18 and npm"},
}

// Languages lists the bundled languages, sorted.
func Languages() []Language {
	var list []Language
	for _, language := range languages {
		list = append(list, language)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return list
}

// ErrUnknownLanguage is a language with no bundled client.
var ErrUnknownLanguage = errors.New("no bundled client for this language")

// ErrNotEmpty is an output folder that already holds something, without force.
var ErrNotEmpty = errors.New("the output folder is not empty")

// Proto is the ibsen.proto the clients were generated from.
func Proto() []byte {
	content, err := bundle.ReadFile("ibsen.proto")
	if err != nil {
		panic(err) // embedded at build time
	}
	return content
}

// ProtoVersion names the proto the clients were generated from: the first 12 hex digits of
// its SHA-256, so two folders from the same proto say the same thing.
func ProtoVersion() string {
	sum := sha256.Sum256(Proto())
	return hex.EncodeToString(sum[:])[:12]
}

// Options are what a generated folder is filled in with.
type Options struct {
	// Package is the Go module path, Java package, Python package or npm package name;
	// empty means the language's default.
	Package string
	// IbsenVersion is recorded in the folder's README and build file.
	IbsenVersion string
	// Force writes into a folder that is not empty.
	Force bool
}

// Generate writes the client for language into dir, creating it, and returns the files it
// wrote, relative to dir.
func Generate(language, dir string, options Options) ([]string, error) {
	lang, ok := languages[language]
	if !ok {
		return nil, fmt.Errorf("%w: %q; the bundled ones are %s", ErrUnknownLanguage, language, names())
	}
	pkg := options.Package
	if pkg == "" {
		pkg = lang.DefaultPackage
	}
	if err := validPackage(language, pkg); err != nil {
		return nil, err
	}
	if !options.Force {
		if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
			return nil, fmt.Errorf("%w: %s; give --force to write into it anyway", ErrNotEmpty, dir)
		}
	}
	replacer := strings.NewReplacer(
		"@@MODULE@@", pkg,
		"@@PACKAGE@@", pkg,
		"@@PACKAGE_PATH@@", strings.ReplaceAll(pkg, ".", "/"),
		"@@IBSEN_VERSION@@", options.IbsenVersion,
		"@@PROTO_VERSION@@", ProtoVersion(),
	)
	root := path.Join("clients", language)
	var written []string
	err := fs.WalkDir(bundle, root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		content, err := bundle.ReadFile(name)
		if err != nil {
			return err
		}
		relative := strings.TrimSuffix(strings.TrimPrefix(name, root+"/"), ".tmpl")
		relative = strings.ReplaceAll(relative, "__package_path__", strings.ReplaceAll(pkg, ".", "/"))
		relative = strings.ReplaceAll(relative, "__package__", pkg)
		target := filepath.Join(dir, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(target, []byte(replacer.Replace(string(content))), 0o644); err != nil {
			return err
		}
		written = append(written, relative)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(written)
	return written, nil
}

func names() string {
	var list []string
	for _, language := range Languages() {
		list = append(list, language.Name)
	}
	return strings.Join(list, ", ")
}

// ErrInvalidPackage is a --package that the language would not accept.
var ErrInvalidPackage = errors.New("invalid package name")

func validPackage(language, pkg string) error {
	identifier := func(part string) bool {
		if part == "" {
			return false
		}
		for i, c := range part {
			letter := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
			if !letter && (i == 0 || c < '0' || c > '9') {
				return false
			}
		}
		return true
	}
	ok := true
	switch language {
	case "java":
		for _, part := range strings.Split(pkg, ".") {
			ok = ok && identifier(part)
		}
	case "python":
		ok = identifier(pkg)
	case "typescript":
		ok = pkg != "" && !strings.ContainsAny(pkg, " \\\"'")
	case "go":
		ok = pkg != "" && !strings.ContainsAny(pkg, " \\\"'") && !strings.HasPrefix(pkg, "/")
	}
	if !ok {
		return fmt.Errorf("%w: %q is not a %s package name", ErrInvalidPackage, pkg, language)
	}
	return nil
}
