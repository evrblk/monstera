package commands

import (
	"fmt"
	"log"
	"os"

	"github.com/evrblk/monstera/x/codegen"
	"github.com/spf13/cobra"
)

var codeGenerateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generates stubs, core interfaces and adapters",
	Run: func(cmd *cobra.Command, args []string) {
		monsteraYaml, err := codegen.LoadMonsteraYaml("./monstera.yaml")
		if err != nil {
			log.Fatalf("failed to load monstera.yaml: %v", err)
		}

		// Generate stubs
		out, err := os.Create("./stubs.go")
		if err != nil {
			log.Fatalf("failed to create output file: %v", err)
		}
		defer out.Close()
		fmt.Fprint(out, codegen.GenerateStubs(monsteraYaml))

		// Generate core api
		out, err = os.Create("./api.go")
		if err != nil {
			log.Fatalf("failed to create output file: %v", err)
		}
		defer out.Close()
		fmt.Fprint(out, codegen.GenerateCoreApis(monsteraYaml))

		// Generate adapters
		out, err = os.Create("./adapters.go")
		if err != nil {
			log.Fatalf("failed to create output file: %v", err)
		}
		defer out.Close()
		fmt.Fprint(out, codegen.GenerateAdapters(monsteraYaml))
	},
}

func init() {
	codeCmd.AddCommand(codeGenerateCmd)
}
