package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/tcw/ibsen/adapter/driver/cli/clientgen"
)

var (
	generateOut     string
	generateForce   bool
	generatePackage string
	generateList    bool

	cmdGenerate = &cobra.Command{
		Use:   "generate",
		Short: "write a ready-to-build client for another language, or the proto",
		Long:  `Writes code generated from Ibsen's gRPC API when this binary was built, so nothing else needs installing: no protoc, no plugins.`,
	}

	cmdGenerateClient = &cobra.Command{
		Use:   "client <language>",
		Short: "write a ready-to-build client folder: go, java, python or typescript",
		Long: `Writes a folder holding the generated gRPC stubs, a build file with pinned versions, a small
client that makes write, read and list easy (with safe retries), a runnable example and a README.
The folder is ./ibsen-client-<language> unless -o says otherwise, and must be empty unless
--force is given.`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if generateList {
				for _, language := range clientgen.Languages() {
					fmt.Printf("%-11s builds with %s; --package defaults to %s\n",
						language.Name, language.Builds, language.DefaultPackage)
				}
				return
			}
			if len(args) != 1 {
				fail(userErrorf("say which language: %s", languageNames()), "")
			}
			if err := runGenerateClient(args[0]); err != nil {
				fail(err, "unable to generate the %s client", args[0])
			}
		},
	}

	cmdGenerateProto = &cobra.Command{
		Use:   "proto",
		Short: "print ibsen.proto, for anyone who wants to run protoc themselves",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			if _, err := os.Stdout.Write(clientgen.Proto()); err != nil {
				fail(err, "unable to write the proto")
			}
		},
	}
)

func runGenerateClient(language string) error {
	dir := generateOut
	if dir == "" {
		dir = "ibsen-client-" + language
	}
	written, err := clientgen.Generate(language, dir, clientgen.Options{
		Package:      generatePackage,
		IbsenVersion: version(),
		Force:        generateForce,
	})
	switch {
	case errors.Is(err, clientgen.ErrUnknownLanguage):
		return userErrorf("there is no bundled client for %q; the bundled ones are %s", language, languageNames())
	case errors.Is(err, clientgen.ErrNotEmpty):
		return userErrorf("%s is not empty; give --force to write into it anyway, or -o another folder", dir)
	case errors.Is(err, clientgen.ErrInvalidPackage):
		return userErrorf("%v", err)
	case err != nil:
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %d files to %s; see %s/README.md\n", len(written), dir, dir)
	return nil
}

func languageNames() string {
	var names []string
	for _, language := range clientgen.Languages() {
		names = append(names, language.Name)
	}
	return strings.Join(names, ", ")
}

func init() {
	cmdGenerateClient.Flags().StringVarP(&generateOut, "out", "o", "", "Folder to write the client to; default ./ibsen-client-<language>")
	cmdGenerateClient.Flags().BoolVarP(&generateForce, "force", "", false, "Write into a folder that is not empty")
	cmdGenerateClient.Flags().StringVarP(&generatePackage, "package", "", "", "Package name: a Java package, a Python package, an npm package or a Go module path")
	cmdGenerateClient.Flags().StringVarP(&generatePackage, "module", "", "", "The Go module path; the same as --package")
	cmdGenerateClient.Flags().BoolVarP(&generateList, "list", "", false, "List the bundled languages")
	cmdGenerate.AddCommand(cmdGenerateClient, cmdGenerateProto)
	rootCmd.AddCommand(cmdGenerate)
}
