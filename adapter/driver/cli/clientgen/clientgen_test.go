package clientgen

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The files each language must hand out: stubs, a build file, the wrapper, an example and a
// README.
var expected = map[string][]string{
	"go": {"go.mod", "ibsenpb/ibsen.pb.go", "ibsenpb/ibsen_grpc.pb.go", "ibsen/client.go", "example/main.go", "README.md"},
	"java": {"pom.xml", "README.md", "src/main/java/github/com/tcw/ibsen/client/IbsenGrpc.java",
		"src/main/java/github/com/tcw/ibsen/client/InputEntries.java",
		"src/main/java/com/example/ibsen/IbsenClient.java", "src/main/java/com/example/ibsen/Example.java"},
	"python":     {"pyproject.toml", "README.md", "example.py", "ibsen_client/__init__.py", "ibsen_client/client.py", "ibsen_client/ibsen_pb2.py", "ibsen_client/ibsen_pb2_grpc.py"},
	"typescript": {"package.json", "tsconfig.json", "README.md", "src/ibsen.ts", "src/client.ts", "src/example.ts"},
}

// placeholder is one of this package's tokens; protoc's own @@protoc_insertion_point is not
var placeholder = regexp.MustCompile(`@@[A-Z_]+@@`)

func TestEveryLanguageWritesItsFolder(t *testing.T) {
	if len(Languages()) != len(expected) {
		t.Fatalf("bundled %d languages, the test knows %d", len(Languages()), len(expected))
	}
	for language, files := range expected {
		t.Run(language, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "out")
			written, err := Generate(language, dir, Options{IbsenVersion: "v9.9.9"})
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range files {
				if _, err := os.Stat(filepath.Join(dir, file)); err != nil {
					t.Errorf("%s is missing: %v", file, err)
				}
			}
			for _, file := range written {
				if strings.HasSuffix(file, ".tmpl") || strings.Contains(file, "__package") {
					t.Errorf("%s kept a template name", file)
				}
				content, err := os.ReadFile(filepath.Join(dir, file))
				if err != nil {
					t.Fatal(err)
				}
				if placeholder.Match(content) {
					t.Errorf("%s still holds a placeholder", file)
				}
			}
			readme, _ := os.ReadFile(filepath.Join(dir, "README.md"))
			if !bytes.Contains(readme, []byte("v9.9.9")) || !bytes.Contains(readme, []byte(ProtoVersion())) {
				t.Error("the README does not record the Ibsen and proto versions it came from")
			}
		})
	}
}

func TestTheBuildFileRecordsTheVersions(t *testing.T) {
	for language, build := range map[string]string{"go": "go.mod", "java": "pom.xml", "python": "pyproject.toml", "typescript": "package.json"} {
		dir := t.TempDir()
		if _, err := Generate(language, dir, Options{IbsenVersion: "v9.9.9"}); err != nil {
			t.Fatal(err)
		}
		content, _ := os.ReadFile(filepath.Join(dir, build))
		if !bytes.Contains(content, []byte("v9.9.9")) || !bytes.Contains(content, []byte(ProtoVersion())) {
			t.Errorf("%s/%s does not record the versions", language, build)
		}
	}
}

func TestAFolderThatIsNotEmptyIsRefusedWithoutForce(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mine.txt"), []byte("keep me"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Generate("go", dir, Options{}); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("got %v, want ErrNotEmpty", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); !os.IsNotExist(err) {
		t.Fatal("something was written into a folder that was refused")
	}
	if _, err := Generate("go", dir, Options{Force: true}); err != nil {
		t.Fatalf("--force was refused: %v", err)
	}
	if content, _ := os.ReadFile(filepath.Join(dir, "mine.txt")); string(content) != "keep me" {
		t.Error("--force touched a file it did not write")
	}
}

func TestTheNamesAreFilledIn(t *testing.T) {
	dir := t.TempDir()
	if _, err := Generate("java", dir, Options{Package: "no.example.logs"}); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(dir, "src/main/java/no/example/logs/IbsenClient.java"))
	if err != nil || !bytes.Contains(content, []byte("package no.example.logs;")) {
		t.Fatalf("the Java package was not filled in: %v", err)
	}
	dir = t.TempDir()
	if _, err := Generate("go", dir, Options{Package: "github.com/someone/logs"}); err != nil {
		t.Fatal(err)
	}
	if content, _ := os.ReadFile(filepath.Join(dir, "go.mod")); !bytes.Contains(content, []byte("module github.com/someone/logs")) {
		t.Error("the Go module was not filled in")
	}
	if content, _ := os.ReadFile(filepath.Join(dir, "example/main.go")); !bytes.Contains(content, []byte(`"github.com/someone/logs/ibsen"`)) {
		t.Error("the Go example does not import the module")
	}
}

func TestUnknownLanguagesAndBadPackagesAreRefused(t *testing.T) {
	if _, err := Generate("cobol", t.TempDir(), Options{}); !errors.Is(err, ErrUnknownLanguage) {
		t.Errorf("got %v, want ErrUnknownLanguage", err)
	}
	for language, bad := range map[string]string{"java": "com.example.1bad", "python": "has-dash", "go": "/rooted"} {
		if _, err := Generate(language, t.TempDir(), Options{Package: bad}); !errors.Is(err, ErrInvalidPackage) {
			t.Errorf("%s package %q: got %v, want ErrInvalidPackage", language, bad, err)
		}
	}
}

// What `ibsen generate proto` prints is the proto in the repository, byte for byte.
func TestTheBundledProtoIsTheRepositorys(t *testing.T) {
	repository, err := os.ReadFile("../../../../api/grpcApi/ibsen.proto")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(Proto(), repository) {
		t.Fatal("the bundled ibsen.proto differs from api/grpcApi/ibsen.proto: run scripts/gen-clients.sh")
	}
}
