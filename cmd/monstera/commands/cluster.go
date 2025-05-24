package commands

import (
	"github.com/spf13/cobra"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster commands",
}

func init() {
	rootCmd.AddCommand(clusterCmd)
}
