package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "roachprod [command] (flags)",
	Short: "roachprod tool for manipulating test clusters",
	Long: `
roachprod is a tool for manipulating test clusters, allowing easy creating,
destroying and wiping of clusters along with running load generators.
`,
}

var (
	numNodes int
	username string
)

var createVMOpts VMOpts

var createCmd = &cobra.Command{
	Use:   "create <cluster id>",
	Short: "create a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("wrong number of arguments")
		}
		clusterID := args[0]

		if len(clusterID) == 0 {
			return fmt.Errorf("cluster ID cannot be blank")
		}
		if numNodes <= 0 || numNodes >= 1000 {
			// Upper limit is just for safety.
			return fmt.Errorf("number of nodes must be in [1..999]")
		}

		account := username
		var err error
		if len(username) == 0 {
			account, err = findActiveAccount()
			if err != nil {
				return err
			}
		}

		clusterName := account + "-" + clusterID
		fmt.Printf("Creating cluster %s with %d nodes\n", clusterName, numNodes)

		cloud, err := listCloud()
		if err != nil {
			return err
		}

		if _, ok := cloud.Clusters[clusterName]; ok {
			return fmt.Errorf("cluster %s already exists", clusterName)
		}

		if err := createCluster(clusterName, numNodes, createVMOpts); err != nil {
			return err
		}

		fmt.Println("OK")

		cloud, err = listCloud()
		if err != nil {
			return err
		}

		c, ok := cloud.Clusters[clusterName]
		if !ok {
			return fmt.Errorf("could not find %s in list of cluster", clusterName)
		}
		c.PrintDetails()

		return syncAll(cloud)
	},
}

var destroyCmd = &cobra.Command{
	Use:   "destroy <cluster ID>",
	Short: "destroy a cluster",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("wrong number of arguments")
		}
		clusterID := args[0]

		if len(clusterID) == 0 {
			return fmt.Errorf("cluster ID cannot be blank")
		}

		account := username
		var err error
		if len(username) == 0 {
			account, err = findActiveAccount()
			if err != nil {
				return err
			}
		}

		clusterName := account + "-" + clusterID

		cloud, err := listCloud()
		if err != nil {
			return err
		}

		c, ok := cloud.Clusters[clusterName]
		if !ok {
			return fmt.Errorf("cluster %s does not exist", clusterName)
		}

		fmt.Printf("Destroying cluster %s with %d nodes\n", clusterName, len(c.VMs))
		if err := destroyCluster(c); err != nil {
			return err
		}

		fmt.Println("OK")
		return nil
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "retrieve the list of clusters",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		cloud, err := listCloud()
		if err != nil {
			return err
		}

		for _, c := range cloud.Clusters {
			c.PrintDetails()
		}

		return syncAll(cloud)
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "retrieve your prod status",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		account, err := findActiveAccount()
		if err != nil {
			return err
		}
		fmt.Printf("Account: %s\n", account)

		cloud, err := listCloud()
		if err != nil {
			return err
		}
		for _, c := range cloud.Clusters {
			fmt.Printf("%s\n", c)
		}

		return nil
	},
}

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "sync ssh keys/config and hosts files",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		cloud, err := listCloud()
		if err != nil {
			return err
		}

		return syncAll(cloud)
	},
}

func syncAll(cloud *Cloud) error {
	fmt.Println("Syncing...")
	if err := initHostDir(); err != nil {
		return err
	}
	if err := syncHosts(cloud); err != nil {
		return err
	}
	if err := cleanSSH(); err != nil {
		return err
	}
	if err := configSSH(); err != nil {
		return err
	}
	return nil
}

func main() {
	cobra.EnableCommandSorting = false

	rootCmd.AddCommand(createCmd, destroyCmd, listCmd, statusCmd, syncCmd)

	createCmd.Flags().DurationVarP(&createVMOpts.Lifetime, "lifetime", "l", 12*time.Hour, "Lifetime of the cluster")
	createCmd.Flags().BoolVar(&createVMOpts.UseLocalSSD, "local-ssd", false, "Use local SSD")
	createCmd.Flags().StringVar(&createVMOpts.MachineType, "machine-type", "n1-standard-4", "Machine type (see https://cloud.google.com/compute/docs/machine-types)")
	createCmd.Flags().IntVarP(&numNodes, "nodes", "n", 3, "Number of nodes")
	createCmd.Flags().StringVarP(&username, "username", "u", "", "Username to run under, detect if blank")

	destroyCmd.Flags().StringVarP(&username, "username", "u", "", "Username to run under, detect if blank")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
