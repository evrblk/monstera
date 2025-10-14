package commands

import (
	"github.com/spf13/cobra"
)

var codeCmd = &cobra.Command{
	Use:   "code",
	Short: "Code generation",
}

func init() {
	rootCmd.AddCommand(codeCmd)
}
