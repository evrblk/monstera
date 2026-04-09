package commands

import (
	"encoding/binary"
	"log"
	"math/rand/v2"

	"github.com/spf13/cobra"

	"github.com/evrblk/monstera/cluster"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Working with cluster configs",
}

var initCmdCfg struct {
	outputConfigPath string
	nodeAddresses    []string
	nodeIds          []string
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Creates a new cluster config",
	Run: func(cmd *cobra.Command, args []string) {
		if len(initCmdCfg.nodeAddresses) != len(initCmdCfg.nodeIds) {
			log.Fatal("node-address and node-id must have the same length")
		}

		config := cluster.CreateEmptyConfig()

		for i := range initCmdCfg.nodeAddresses {
			_, err := config.CreateNode(initCmdCfg.nodeIds[i], initCmdCfg.nodeAddresses[i])
			if err != nil {
				log.Fatal(err)
			}
		}

		err := config.Validate()
		if err != nil {
			log.Fatal(err)
		}

		err = cluster.WriteConfigToFile(config, initCmdCfg.outputConfigPath)
		if err != nil {
			log.Fatal(err)
		}
	},
}

var addNodeCmdCfg struct {
	inputConfigPath  string
	outputConfigPath string
	nodeAddress      string
	nodeId           string
}

var addNodeCmd = &cobra.Command{
	Use:   "add-node",
	Short: "Adds node to the cluster config",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := cluster.LoadConfigFromFile(addNodeCmdCfg.inputConfigPath)
		if err != nil {
			log.Fatal(err)
		}

		_, err = config.CreateNode(addNodeCmdCfg.nodeId, addNodeCmdCfg.nodeAddress)
		if err != nil {
			log.Fatal(err)
		}

		config.IncrementVersion()

		err = config.Validate()
		if err != nil {
			log.Fatal(err)
		}

		output := addNodeCmdCfg.outputConfigPath
		if output == "" {
			output = addNodeCmdCfg.inputConfigPath
		}
		err = cluster.WriteConfigToFile(config, output)
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
		config, err := cluster.LoadConfigFromFile(addApplicationCmdCfg.inputConfigPath)
		if err != nil {
			log.Fatal(err)
		}

		_, err = config.CreateApplication(addApplicationCmdCfg.applicationName, addApplicationCmdCfg.implementation, int32(addApplicationCmdCfg.replicationFactor))
		if err != nil {
			log.Fatal(err)
		}

		nodes := make([]string, len(config.Nodes))
		for i := range config.Nodes {
			nodes[i] = config.Nodes[i].Id
		}

		shardSize := cluster.KeyspacePerApplication / addApplicationCmdCfg.shardsCount
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
			rand.Shuffle(len(shuffledNodes), func(i, j int) {
				shuffledNodes[i], shuffledNodes[j] = shuffledNodes[j], shuffledNodes[i]
			})

			for j := 0; j < addApplicationCmdCfg.replicationFactor; j++ {
				_, err := config.CreateReplica(addApplicationCmdCfg.applicationName, shard.Id, shuffledNodes[j])
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		config.IncrementVersion()

		err = config.Validate()
		if err != nil {
			log.Fatal(err)
		}

		output := addApplicationCmdCfg.outputConfigPath
		if output == "" {
			output = addApplicationCmdCfg.inputConfigPath
		}
		err = cluster.WriteConfigToFile(config, output)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(configCmd)

	// config init
	configCmd.AddCommand(initCmd)
	initCmd.PersistentFlags().StringVarP(&initCmdCfg.outputConfigPath, "output", "", "", "Monstera cluster config output path")
	panicIfNotNil(initCmd.MarkPersistentFlagRequired("output"))
	initCmd.PersistentFlags().StringArrayVarP(&initCmdCfg.nodeAddresses, "node-address", "", nil, "Cluster node addresses (host:port), minimum 3")
	panicIfNotNil(initCmd.MarkPersistentFlagRequired("node-address"))
	initCmd.PersistentFlags().StringArrayVarP(&initCmdCfg.nodeIds, "node-id", "", nil, "Cluster node IDs, minimum 3")
	panicIfNotNil(initCmd.MarkPersistentFlagRequired("node-id"))

	// config add-node
	configCmd.AddCommand(addNodeCmd)
	addNodeCmd.PersistentFlags().StringVarP(&addNodeCmdCfg.outputConfigPath, "output", "", "", "Monstera cluster config output path")
	addNodeCmd.PersistentFlags().StringVarP(&addNodeCmdCfg.inputConfigPath, "config", "", "", "Monstera cluster config input path")
	panicIfNotNil(addNodeCmd.MarkPersistentFlagRequired("config"))
	addNodeCmd.PersistentFlags().StringVarP(&addNodeCmdCfg.nodeAddress, "node-address", "", "", "Cluster node address (host:port)")
	panicIfNotNil(addNodeCmd.MarkPersistentFlagRequired("node-address"))
	addNodeCmd.PersistentFlags().StringVarP(&addNodeCmdCfg.nodeId, "node-id", "", "", "Cluster node ID")
	panicIfNotNil(addNodeCmd.MarkPersistentFlagRequired("node-id"))

	// config add-application
	configCmd.AddCommand(addApplicationCmd)
	addApplicationCmd.PersistentFlags().StringVarP(&addApplicationCmdCfg.outputConfigPath, "output", "", "", "Monstera cluster config output path")
	addApplicationCmd.PersistentFlags().StringVarP(&addApplicationCmdCfg.inputConfigPath, "config", "", "", "Monstera cluster config input path")
	panicIfNotNil(addApplicationCmd.MarkPersistentFlagRequired("config"))
	addApplicationCmd.PersistentFlags().StringVarP(&addApplicationCmdCfg.applicationName, "name", "", "", "Application name")
	panicIfNotNil(addApplicationCmd.MarkPersistentFlagRequired("name"))
	addApplicationCmd.PersistentFlags().StringVarP(&addApplicationCmdCfg.implementation, "implementation", "", "", "Application implementation")
	panicIfNotNil(addApplicationCmd.MarkPersistentFlagRequired("implementation"))
	addApplicationCmd.PersistentFlags().IntVarP(&addApplicationCmdCfg.replicationFactor, "replication-factor", "", 3, "Replication factor")
	addApplicationCmd.PersistentFlags().IntVarP(&addApplicationCmdCfg.shardsCount, "shards-count", "", 16, "Shards count (power of 2)")
}

func panicIfNotNil(err error) {
	if err != nil {
		panic(err)
	}
}
