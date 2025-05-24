package commands

import (
	"github.com/spf13/cobra"
)

var codeCmd = &cobra.Command{
	Use:   "code",
	Short: "Generates code",
}

func init() {
	rootCmd.AddCommand(codeCmd)
}
