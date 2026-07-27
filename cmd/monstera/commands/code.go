package commands

import (
	"log"
	"os"

	"github.com/spf13/cobra"

	"github.com/evrblk/monstera/rpc/codegen"
)

var codeCmd = &cobra.Command{
	Use:   "code",
	Short: "Code generation",
}

var codeGenerateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generates RPC stubs, core interfaces and adapters",
	Long: `Generates RPC stubs, core interfaces and adapters from ./monstera.yaml.

Run it from the directory containing monstera.yaml. It overwrites ./stubs.go,
./api.go, and ./adapters.go in that directory. All three files are generated
in memory first: if the manifest is invalid or generation fails, existing
output files are left untouched.`,
	Run: func(cmd *cobra.Command, args []string) {
		monsteraYaml, err := codegen.LoadMonsteraYaml("./monstera.yaml")
		if err != nil {
			log.Fatalf("failed to load monstera.yaml: %v", err)
		}

		// Generate everything in memory first so a failure cannot leave
		// existing output files truncated or half-updated.
		stubs, err := codegen.GenerateStubs(monsteraYaml)
		if err != nil {
			log.Fatalf("failed to generate stubs: %v", err)
		}
		apis, err := codegen.GenerateCoreApis(monsteraYaml)
		if err != nil {
			log.Fatalf("failed to generate core APIs: %v", err)
		}
		adapters, err := codegen.GenerateAdapters(monsteraYaml)
		if err != nil {
			log.Fatalf("failed to generate adapters: %v", err)
		}

		for _, file := range []struct {
			path    string
			content string
		}{
			{"./stubs.go", stubs},
			{"./api.go", apis},
			{"./adapters.go", adapters},
		} {
			if err := os.WriteFile(file.path, []byte(file.content), 0644); err != nil {
				log.Fatalf("failed to write %s: %v", file.path, err)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(codeCmd)
	codeCmd.AddCommand(codeGenerateCmd)
}
