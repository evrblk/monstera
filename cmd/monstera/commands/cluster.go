package commands

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/control"
	monsteragrpc "github.com/evrblk/monstera/transport/grpc"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Operating a running Monstera cluster",
}

var bootstrapNodeCmdCfg struct {
	configPath  string
	nodeId      string
	nodeAddress string
	timeout     time.Duration
}

var bootstrapNodeCmd = &cobra.Command{
	Use:   "bootstrap-node",
	Short: "Provisions an unprovisioned node with its id and the initial cluster config",
	Long: "Bootstraps a single UNPROVISIONED Monstera node over the admin plane. It dials the node " +
		"by address and installs the given cluster config, assigning it --node-id (which must name a " +
		"node present in the config). This is how a fresh node — started with only a data dir — is " +
		"provisioned. It is idempotent: re-running it against a node already provisioned with the " +
		"same id is a no-op (a different id is rejected — identity is immutable).",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := cluster.LoadConfigFromFile(bootstrapNodeCmdCfg.configPath)
		if err != nil {
			log.Fatalf("loading cluster config: %v", err)
		}

		// The node must be in the config; use its advertised address unless overridden.
		node, err := config.GetNode(bootstrapNodeCmdCfg.nodeId)
		if err != nil {
			log.Fatalf("node %q not found in cluster config: %v", bootstrapNodeCmdCfg.nodeId, err)
		}
		address := bootstrapNodeCmdCfg.nodeAddress
		if address == "" {
			address = node.GrpcAddress
		}

		admin := monsteragrpc.NewAdminClient()
		defer admin.Close()

		ctx, cancel := context.WithTimeout(context.Background(), bootstrapNodeCmdCfg.timeout)
		defer cancel()

		log.Printf("Bootstrapping node %q at %s with cluster config version %d...", bootstrapNodeCmdCfg.nodeId, address, config.Version)
		if err := admin.Bootstrap(ctx, address, bootstrapNodeCmdCfg.nodeId, config); err != nil {
			log.Fatalf("bootstrap failed: %v", err)
		}
		log.Printf("Node %q bootstrapped.", bootstrapNodeCmdCfg.nodeId)
	},
}

var bootstrapNodesCmdCfg struct {
	configPath string
	timeout    time.Duration
}

var bootstrapNodesCmd = &cobra.Command{
	Use:   "bootstrap-nodes",
	Short: "Provisions every node in the cluster config",
	Long: "Bootstraps all nodes in the given cluster config over the admin plane, dialing each at its " +
		"advertised GrpcAddress and assigning it its configured id. Use this to stand up a fresh " +
		"cluster in one step. It attempts every node and reports a summary; bootstrapping is " +
		"idempotent, so re-running it (e.g. after some nodes were already provisioned) is safe.",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := cluster.LoadConfigFromFile(bootstrapNodesCmdCfg.configPath)
		if err != nil {
			log.Fatalf("loading cluster config: %v", err)
		}

		admin := monsteragrpc.NewAdminClient()
		defer admin.Close()

		var failed int
		for _, node := range config.Nodes {
			log.Printf("Bootstrapping node %q at %s with cluster config version %d...", node.Id, node.GrpcAddress, config.Version)
			ctx, cancel := context.WithTimeout(context.Background(), bootstrapNodesCmdCfg.timeout)
			err := admin.Bootstrap(ctx, node.GrpcAddress, node.Id, config)
			cancel()
			if err != nil {
				log.Printf("  node %q: bootstrap failed: %v", node.Id, err)
				failed++
				continue
			}
			log.Printf("  node %q: bootstrapped", node.Id)
		}

		if failed > 0 {
			log.Fatalf("%d of %d nodes failed to bootstrap", failed, len(config.Nodes))
		}
		log.Printf("All %d nodes bootstrapped.", len(config.Nodes))
	},
}

var clusterAddNodeCmdCfg struct {
	configPath   string
	nodeId       string
	nodeAddress  string
	sequencePath string
	timeout      time.Duration
}

var clusterAddNodeCmd = &cobra.Command{
	Use:   "add-node",
	Short: "Adds a node to a running cluster via a control sequence",
	Long: "Plans and runs an add-node control sequence over the admin plane: it installs a new " +
		"cluster config (base version + 1, with the node added) on every existing node and " +
		"bootstraps the new node. The new node process must already be running (blank data dir, with " +
		"--listen) so the sequence can bootstrap it at its advertised address.\n\n" +
		"Progress is checkpointed to --sequence (a temp file keyed by node id if omitted); re-run " +
		"with the same --config and --sequence to resume after an interruption.",
	Run: func(cmd *cobra.Command, args []string) {
		base, err := cluster.LoadConfigFromFile(clusterAddNodeCmdCfg.configPath)
		if err != nil {
			log.Fatalf("loading cluster config: %v", err)
		}

		seqPath := clusterAddNodeCmdCfg.sequencePath
		if seqPath == "" {
			seqPath = filepath.Join(os.TempDir(), fmt.Sprintf("monstera-add-node-%s.json", clusterAddNodeCmdCfg.nodeId))
		}

		// Resume an existing sequence at this path, otherwise plan a fresh one.
		var seq *control.Sequence
		if _, statErr := os.Stat(seqPath); statErr == nil {
			seq, err = control.LoadSequence(seqPath)
			if err != nil {
				log.Fatalf("loading sequence %s: %v", seqPath, err)
			}
			log.Printf("Resuming sequence %q from %s (step %d/%d)", seq.Name, seqPath, seq.Cursor+1, len(seq.Steps))
		} else {
			seq, err = control.PlanAddNode(base, clusterAddNodeCmdCfg.nodeId, clusterAddNodeCmdCfg.nodeAddress)
			if err != nil {
				log.Fatalf("planning add-node: %v", err)
			}
			seq.CreatedAt = time.Now().UTC().Format(time.RFC3339)
			if err := control.SaveSequence(seqPath, seq); err != nil {
				log.Fatalf("saving sequence: %v", err)
			}
			log.Printf("Planned add-node sequence at %s", seqPath)
		}

		admin := monsteragrpc.NewAdminClient()
		defer admin.Close()

		exec := control.NewExecutor(admin, base, seqPath, control.DefaultOptions())
		ctx, cancel := context.WithTimeout(context.Background(), clusterAddNodeCmdCfg.timeout)
		defer cancel()

		if err := exec.Run(ctx, seq); err != nil {
			log.Fatalf("add-node failed: %v", err)
		}
		log.Printf("Node %q added; cluster is at version %d.", clusterAddNodeCmdCfg.nodeId, base.Version+1)
	},
}

var moveShardCmdCfg struct {
	configPath   string
	shardId      string
	fromNode     string
	toNode       string
	bake         time.Duration
	sequencePath string
	timeout      time.Duration
}

var moveShardCmd = &cobra.Command{
	Use:   "move-shard",
	Short: "Moves a shard replica from one node to another via a control sequence",
	Long: "Plans and runs a move-shard control sequence over the admin plane: it adds a new replica " +
		"of the shard on --to-node, waits for it to catch up, bakes (soaks) for --bake while it " +
		"stabilizes, then removes the replica on --from-node (transferring leadership away first if " +
		"that replica leads). Replication factor is preserved, with a transient extra voter during " +
		"the bake (safe for RF>=3).\n\n" +
		"Progress is checkpointed to --sequence (a temp file if omitted); re-run with the same " +
		"--config and --sequence to resume after an interruption.",
	Run: func(cmd *cobra.Command, args []string) {
		base, err := cluster.LoadConfigFromFile(moveShardCmdCfg.configPath)
		if err != nil {
			log.Fatalf("loading cluster config: %v", err)
		}

		seqPath := moveShardCmdCfg.sequencePath
		if seqPath == "" {
			seqPath = filepath.Join(os.TempDir(), fmt.Sprintf("monstera-move-shard-%s-%s-%s.json", moveShardCmdCfg.shardId, moveShardCmdCfg.fromNode, moveShardCmdCfg.toNode))
		}

		var seq *control.Sequence
		if _, statErr := os.Stat(seqPath); statErr == nil {
			seq, err = control.LoadSequence(seqPath)
			if err != nil {
				log.Fatalf("loading sequence %s: %v", seqPath, err)
			}
			log.Printf("Resuming sequence %q from %s (step %d/%d)", seq.Name, seqPath, seq.Cursor+1, len(seq.Steps))
		} else {
			seq, err = control.PlanMoveShard(base, moveShardCmdCfg.shardId, moveShardCmdCfg.fromNode, moveShardCmdCfg.toNode, moveShardCmdCfg.bake)
			if err != nil {
				log.Fatalf("planning move-shard: %v", err)
			}
			seq.CreatedAt = time.Now().UTC().Format(time.RFC3339)
			if err := control.SaveSequence(seqPath, seq); err != nil {
				log.Fatalf("saving sequence: %v", err)
			}
			log.Printf("Planned move-shard sequence at %s", seqPath)
		}

		admin := monsteragrpc.NewAdminClient()
		defer admin.Close()

		exec := control.NewExecutor(admin, base, seqPath, control.DefaultOptions())
		ctx, cancel := context.WithTimeout(context.Background(), moveShardCmdCfg.timeout)
		defer cancel()

		if err := exec.Run(ctx, seq); err != nil {
			log.Fatalf("move-shard failed: %v", err)
		}
		log.Printf("Shard %q moved from %q to %q; cluster is at version %d.", moveShardCmdCfg.shardId, moveShardCmdCfg.fromNode, moveShardCmdCfg.toNode, base.Version+2)
	},
}

var splitShardCmdCfg struct {
	configPath   string
	shardId      string
	splitAt      string
	bake         time.Duration
	sequencePath string
	timeout      time.Duration
}

var splitShardCmd = &cobra.Command{
	Use:   "split-shard",
	Short: "Splits an active shard into two children via a control sequence",
	Long: "Plans and runs a split-shard control sequence over the admin plane: it declares the split " +
		"(parent -> splitting, two activating children co-located with the parent's replicas), waits " +
		"for every node to seed its children, delivers the CUTOFF through the parent's Raft log " +
		"(freezing the parent and promoting the children with zero write downtime), flips the config " +
		"(parent -> inactive, children -> active), then bakes for --bake.\n\n" +
		"--split-at is the first shard key of the second child, 8 hex characters (4 bytes), e.g. " +
		"80000000 for an even split of a full-range shard.\n\n" +
		"Progress is checkpointed to --sequence (a temp file if omitted); re-run with the same " +
		"--config and --sequence to resume after an interruption.",
	Run: func(cmd *cobra.Command, args []string) {
		base, err := cluster.LoadConfigFromFile(splitShardCmdCfg.configPath)
		if err != nil {
			log.Fatalf("loading cluster config: %v", err)
		}

		splitAtBytes, err := hex.DecodeString(splitShardCmdCfg.splitAt)
		if err != nil {
			log.Fatalf("parsing --split-at: %v", err)
		}
		splitAt, err := cluster.ShardKeyFromBytes(splitAtBytes)
		if err != nil {
			log.Fatalf("parsing --split-at: %v (expected 8 hex characters, e.g. 80000000)", err)
		}

		seqPath := splitShardCmdCfg.sequencePath
		if seqPath == "" {
			seqPath = filepath.Join(os.TempDir(), fmt.Sprintf("monstera-split-shard-%s-%s.json", splitShardCmdCfg.shardId, splitShardCmdCfg.splitAt))
		}

		var seq *control.Sequence
		if _, statErr := os.Stat(seqPath); statErr == nil {
			seq, err = control.LoadSequence(seqPath)
			if err != nil {
				log.Fatalf("loading sequence %s: %v", seqPath, err)
			}
			log.Printf("Resuming sequence %q from %s (step %d/%d)", seq.Name, seqPath, seq.Cursor+1, len(seq.Steps))
		} else {
			seq, err = control.PlanSplitShard(base, splitShardCmdCfg.shardId, splitAt, splitShardCmdCfg.bake)
			if err != nil {
				log.Fatalf("planning split-shard: %v", err)
			}
			seq.CreatedAt = time.Now().UTC().Format(time.RFC3339)
			if err := control.SaveSequence(seqPath, seq); err != nil {
				log.Fatalf("saving sequence: %v", err)
			}
			log.Printf("Planned split-shard sequence at %s", seqPath)
		}

		admin := monsteragrpc.NewAdminClient()
		defer admin.Close()

		exec := control.NewExecutor(admin, base, seqPath, control.DefaultOptions())
		ctx, cancel := context.WithTimeout(context.Background(), splitShardCmdCfg.timeout)
		defer cancel()

		if err := exec.Run(ctx, seq); err != nil {
			log.Fatalf("split-shard failed: %v", err)
		}
		log.Printf("Shard %q split at %s; cluster is at version %d.", splitShardCmdCfg.shardId, splitShardCmdCfg.splitAt, base.Version+2)
	},
}

var getConfigCmdCfg struct {
	nodeAddress string
	out         string
	timeout     time.Duration
}

var getConfigCmd = &cobra.Command{
	Use:   "get-config",
	Short: "Downloads the current cluster config from a node",
	Long: "Fetches the cluster config a node is currently running (over the admin plane, by address) " +
		"and prints it as JSON to stdout, or writes it to --out (format chosen by extension: .json or " +
		".pb). The running cluster is the source of truth, so use this to capture the live config after " +
		"a control command — e.g. to feed the pinned base into the next `monstera cluster` command.\n\n" +
		"Progress goes to stderr, so `get-config ... | jq` and `get-config ... > cluster.json` are safe.",
	Run: func(cmd *cobra.Command, args []string) {
		admin := monsteragrpc.NewAdminClient()
		defer admin.Close()

		ctx, cancel := context.WithTimeout(context.Background(), getConfigCmdCfg.timeout)
		defer cancel()

		config, err := admin.GetClusterConfig(ctx, getConfigCmdCfg.nodeAddress)
		if err != nil {
			log.Fatalf("fetching cluster config from %s: %v", getConfigCmdCfg.nodeAddress, err)
		}
		if config == nil {
			log.Fatalf("node %s returned no cluster config (is it provisioned?)", getConfigCmdCfg.nodeAddress)
		}

		if getConfigCmdCfg.out == "" {
			data, err := cluster.WriteConfigToJson(config)
			if err != nil {
				log.Fatalf("encoding config: %v", err)
			}
			fmt.Println(string(data))
			return
		}

		if err := cluster.WriteConfigToFile(config, getConfigCmdCfg.out); err != nil {
			log.Fatalf("writing config to %s: %v", getConfigCmdCfg.out, err)
		}
		log.Printf("Downloaded cluster config version %d to %s", config.Version, getConfigCmdCfg.out)
	},
}

func init() {
	rootCmd.AddCommand(clusterCmd)

	clusterCmd.AddCommand(bootstrapNodeCmd)
	bootstrapNodeCmd.PersistentFlags().StringVarP(&bootstrapNodeCmdCfg.configPath, "config", "", "", "Monstera cluster config path")
	panicIfNotNil(bootstrapNodeCmd.MarkPersistentFlagRequired("config"))
	bootstrapNodeCmd.PersistentFlags().StringVarP(&bootstrapNodeCmdCfg.nodeId, "node-id", "", "", "ID of the node to bootstrap (must exist in the config)")
	panicIfNotNil(bootstrapNodeCmd.MarkPersistentFlagRequired("node-id"))
	bootstrapNodeCmd.PersistentFlags().StringVarP(&bootstrapNodeCmdCfg.nodeAddress, "node-address", "", "", "gRPC address to dial (defaults to the node's GrpcAddress in the config)")
	bootstrapNodeCmd.PersistentFlags().DurationVarP(&bootstrapNodeCmdCfg.timeout, "timeout", "", 30*time.Second, "Bootstrap RPC timeout")

	clusterCmd.AddCommand(bootstrapNodesCmd)
	bootstrapNodesCmd.PersistentFlags().StringVarP(&bootstrapNodesCmdCfg.configPath, "config", "", "", "Monstera cluster config path")
	panicIfNotNil(bootstrapNodesCmd.MarkPersistentFlagRequired("config"))
	bootstrapNodesCmd.PersistentFlags().DurationVarP(&bootstrapNodesCmdCfg.timeout, "timeout", "", 30*time.Second, "Per-node bootstrap RPC timeout")

	clusterCmd.AddCommand(clusterAddNodeCmd)
	clusterAddNodeCmd.PersistentFlags().StringVarP(&clusterAddNodeCmdCfg.configPath, "config", "", "", "Base Monstera cluster config path (the pinned base for the sequence)")
	panicIfNotNil(clusterAddNodeCmd.MarkPersistentFlagRequired("config"))
	clusterAddNodeCmd.PersistentFlags().StringVarP(&clusterAddNodeCmdCfg.nodeId, "node-id", "", "", "ID of the node to add")
	panicIfNotNil(clusterAddNodeCmd.MarkPersistentFlagRequired("node-id"))
	clusterAddNodeCmd.PersistentFlags().StringVarP(&clusterAddNodeCmdCfg.nodeAddress, "node-address", "", "", "Advertised gRPC address (host:port) of the new node")
	panicIfNotNil(clusterAddNodeCmd.MarkPersistentFlagRequired("node-address"))
	clusterAddNodeCmd.PersistentFlags().StringVarP(&clusterAddNodeCmdCfg.sequencePath, "sequence", "", "", "Path to checkpoint/resume the sequence (default: a temp file keyed by node id)")
	clusterAddNodeCmd.PersistentFlags().DurationVarP(&clusterAddNodeCmdCfg.timeout, "timeout", "", 2*time.Minute, "Overall deadline for the sequence")

	clusterCmd.AddCommand(moveShardCmd)
	moveShardCmd.PersistentFlags().StringVarP(&moveShardCmdCfg.configPath, "config", "", "", "Base Monstera cluster config path (the pinned base for the sequence)")
	panicIfNotNil(moveShardCmd.MarkPersistentFlagRequired("config"))
	moveShardCmd.PersistentFlags().StringVarP(&moveShardCmdCfg.shardId, "shard-id", "", "", "ID of the shard to move")
	panicIfNotNil(moveShardCmd.MarkPersistentFlagRequired("shard-id"))
	moveShardCmd.PersistentFlags().StringVarP(&moveShardCmdCfg.fromNode, "from-node", "", "", "ID of the node to move the replica off of")
	panicIfNotNil(moveShardCmd.MarkPersistentFlagRequired("from-node"))
	moveShardCmd.PersistentFlags().StringVarP(&moveShardCmdCfg.toNode, "to-node", "", "", "ID of the node to move the replica to")
	panicIfNotNil(moveShardCmd.MarkPersistentFlagRequired("to-node"))
	moveShardCmd.PersistentFlags().DurationVarP(&moveShardCmdCfg.bake, "bake", "", 30*time.Second, "Soak time after the new replica catches up, before removing the old one")
	moveShardCmd.PersistentFlags().StringVarP(&moveShardCmdCfg.sequencePath, "sequence", "", "", "Path to checkpoint/resume the sequence (default: a temp file)")
	moveShardCmd.PersistentFlags().DurationVarP(&moveShardCmdCfg.timeout, "timeout", "", 5*time.Minute, "Overall deadline for the sequence")

	clusterCmd.AddCommand(splitShardCmd)
	splitShardCmd.PersistentFlags().StringVarP(&splitShardCmdCfg.configPath, "config", "", "", "Base Monstera cluster config path (the pinned base for the sequence)")
	panicIfNotNil(splitShardCmd.MarkPersistentFlagRequired("config"))
	splitShardCmd.PersistentFlags().StringVarP(&splitShardCmdCfg.shardId, "shard-id", "", "", "ID of the (active) shard to split")
	panicIfNotNil(splitShardCmd.MarkPersistentFlagRequired("shard-id"))
	splitShardCmd.PersistentFlags().StringVarP(&splitShardCmdCfg.splitAt, "split-at", "", "", "First shard key of the second child, 8 hex chars (e.g. 80000000)")
	panicIfNotNil(splitShardCmd.MarkPersistentFlagRequired("split-at"))
	splitShardCmd.PersistentFlags().DurationVarP(&splitShardCmdCfg.bake, "bake", "", 30*time.Second, "Soak time after the flip, before declaring the split done")
	splitShardCmd.PersistentFlags().StringVarP(&splitShardCmdCfg.sequencePath, "sequence", "", "", "Path to checkpoint/resume the sequence (default: a temp file)")
	splitShardCmd.PersistentFlags().DurationVarP(&splitShardCmdCfg.timeout, "timeout", "", 10*time.Minute, "Overall deadline for the sequence")

	clusterCmd.AddCommand(getConfigCmd)
	getConfigCmd.PersistentFlags().StringVarP(&getConfigCmdCfg.nodeAddress, "node-address", "", "", "gRPC address (host:port) of a node to fetch the config from")
	panicIfNotNil(getConfigCmd.MarkPersistentFlagRequired("node-address"))
	getConfigCmd.PersistentFlags().StringVarP(&getConfigCmdCfg.out, "out", "", "", "Write the config to this file (format by extension); default: print JSON to stdout")
	getConfigCmd.PersistentFlags().DurationVarP(&getConfigCmdCfg.timeout, "timeout", "", 30*time.Second, "RPC timeout")
}
