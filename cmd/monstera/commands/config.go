package commands

import (
	"encoding/binary"
	"log"

	"github.com/evrblk/monstera"
	"github.com/samber/lo/mutable"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Working with cluster configs",
}

var initCmdCfg struct {
	outputConfigPath string
	nodeAddresses    []string
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Creates a new cluster config",
	Run: func(cmd *cobra.Command, args []string) {
		config := monstera.CreateEmptyConfig()

		for _, n := range initCmdCfg.nodeAddresses {
			_, err := config.CreateNode(n)
			if err != nil {
				log.Fatal(err)
			}
		}

		err := config.Validate()
		if err != nil {
			log.Fatal(err)
		}

		err = monstera.WriteConfigToFile(config, initCmdCfg.outputConfigPath)
		if err != nil {
			log.Fatal(err)
		}
	},
}

var addNodeCmdCfg struct {
	inputConfigPath  string
	outputConfigPath string
	nodeAddress      string
}

var addNodeCmd = &cobra.Command{
	Use:   "add-node",
	Short: "Adds node to the cluster config",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := monstera.LoadConfigFromFile(addNodeCmdCfg.inputConfigPath)
		if err != nil {
			log.Fatal(err)
		}

		_, err = config.CreateNode(addNodeCmdCfg.nodeAddress)
		if err != nil {
			log.Fatal(err)
		}

		err = config.Validate()
		if err != nil {
			log.Fatal(err)
		}

		output := addNodeCmdCfg.outputConfigPath
		if output == "" {
			output = addNodeCmdCfg.inputConfigPath
		}
		err = monstera.WriteConfigToFile(config, output)
		if err != nil {
			log.Fatal(err)
		}
	},
}

var addApplicationCmdCfg struct {
	inputConfigPath   string
	outputConfigPath  string
	applicationName   string
	implementation    string
	replicationFactor int
	shardsCount       int
}

var addApplicationCmd = &cobra.Command{
	Use:   "add-application",
	Short: "Adds application to the cluster config",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := monstera.LoadConfigFromFile(addApplicationCmdCfg.inputConfigPath)
		if err != nil {
			log.Fatal(err)
		}

		_, err = config.CreateApplication(addApplicationCmdCfg.applicationName, addApplicationCmdCfg.implementation, int32(addApplicationCmdCfg.replicationFactor))
		if err != nil {
			log.Fatal(err)
		}

		nodes := make([]string, len(config.Nodes))
		for i := range config.Nodes {
			nodes[i] = config.Nodes[i].Address
		}

		shardSize := monstera.KeyspacePerApplication / addApplicationCmdCfg.shardsCount
		for i := 0; i < addApplicationCmdCfg.shardsCount; i++ {
			lower := uint32(i * shardSize)
			upper := uint32((i+1)*shardSize - 1)
			lowerBound := make([]byte, 4)
			upperBound := make([]byte, 4)
			binary.BigEndian.PutUint32(lowerBound, lower)
			binary.BigEndian.PutUint32(upperBound, upper)

			shard, err := config.CreateShard(addApplicationCmdCfg.applicationName, lowerBound, upperBound, "")
			if err != nil {
				log.Fatal(err)
			}

			shuffledNodes := make([]string, len(nodes))
			copy(shuffledNodes, nodes)
			mutable.Shuffle(shuffledNodes)

			for j := 0; j < addApplicationCmdCfg.replicationFactor; j++ {
				_, err := config.CreateReplica(addApplicationCmdCfg.applicationName, shard.Id, shuffledNodes[j])
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		err = config.Validate()
		if err != nil {
			log.Fatal(err)
		}

		output := addApplicationCmdCfg.outputConfigPath
		if output == "" {
			output = addApplicationCmdCfg.inputConfigPath
		}
		err = monstera.WriteConfigToFile(config, output)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(configCmd)

	configCmd.AddCommand(initCmd)

	initCmd.PersistentFlags().StringVarP(&initCmdCfg.outputConfigPath, "output", "", "", "Monstera cluster config output path")

	initCmd.PersistentFlags().StringArrayVarP(&initCmdCfg.nodeAddresses, "node", "", nil, "Cluster node addresses (host:port), minimum 3")
	err := initCmd.MarkPersistentFlagRequired("node")
	if err != nil {
		panic(err)
	}

	configCmd.AddCommand(addNodeCmd)

	addNodeCmd.PersistentFlags().StringVarP(&addNodeCmdCfg.outputConfigPath, "output", "", "", "Monstera cluster config output path")

	addNodeCmd.PersistentFlags().StringVarP(&addNodeCmdCfg.inputConfigPath, "config", "", "", "Monstera cluster config input path")
	err = addNodeCmd.MarkPersistentFlagRequired("config")
	if err != nil {
		panic(err)
	}

	addNodeCmd.PersistentFlags().StringVarP(&addNodeCmdCfg.nodeAddress, "node", "", "", "Cluster node address (host:port)")
	err = addNodeCmd.MarkPersistentFlagRequired("node")
	if err != nil {
		panic(err)
	}

	configCmd.AddCommand(addApplicationCmd)

	addApplicationCmd.PersistentFlags().StringVarP(&addApplicationCmdCfg.outputConfigPath, "output", "", "", "Monstera cluster config output path")

	addApplicationCmd.PersistentFlags().StringVarP(&addApplicationCmdCfg.inputConfigPath, "config", "", "", "Monstera cluster config input path")
	err = addApplicationCmd.MarkPersistentFlagRequired("config")
	if err != nil {
		panic(err)
	}

	addApplicationCmd.PersistentFlags().StringVarP(&addApplicationCmdCfg.applicationName, "name", "", "", "Application name")
	err = addApplicationCmd.MarkPersistentFlagRequired("name")
	if err != nil {
		panic(err)
	}

	addApplicationCmd.PersistentFlags().StringVarP(&addApplicationCmdCfg.implementation, "implementation", "", "", "Application implementation")
	err = addApplicationCmd.MarkPersistentFlagRequired("implementation")
	if err != nil {
		panic(err)
	}

	addApplicationCmd.PersistentFlags().IntVarP(&addApplicationCmdCfg.replicationFactor, "replication-factor", "", 3, "Replication factor")

	addApplicationCmd.PersistentFlags().IntVarP(&addApplicationCmdCfg.shardsCount, "shards-count", "", 16, "Shards count (power of 2)")
}
