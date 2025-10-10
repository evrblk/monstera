package commands

import (
	"fmt"
	"log"

	"github.com/evrblk/monstera"
	"github.com/spf13/cobra"
)

var monsteraConfigPath string

var printConfigCmd = &cobra.Command{
	Use:   "print-config",
	Short: "Print Monstera cluster config",
	Run: func(cmd *cobra.Command, args []string) {
		clusterConfig, err := monstera.LoadConfigFromFile(monsteraConfigPath)
		if err != nil {
			log.Fatal(err)
		}

		data, err := monstera.WriteConfigToJson(clusterConfig)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(string(data))
	},
}

func init() {
	clusterCmd.AddCommand(printConfigCmd)

	printConfigCmd.PersistentFlags().StringVarP(&monsteraConfigPath, "monstera-config", "m", "", "Monstera cluster config")
	err := printConfigCmd.MarkPersistentFlagRequired("monstera-config")
	if err != nil {
		panic(err)
	}
}
