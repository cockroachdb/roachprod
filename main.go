package main

import (
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	// Note that nearly all uses of this package will be replaced
	// by calls to an instance of a CloudProvider
	cld "github.com/cockroachdb/roachprod/cloud"
	"github.com/cockroachdb/roachprod/config"
	"github.com/cockroachdb/roachprod/install"
	"github.com/cockroachdb/roachprod/ssh"
	"github.com/cockroachdb/roachprod/ui"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

var rootCmd = &cobra.Command{
	Use:   "roachprod [command] (flags)",
	Short: "roachprod tool for manipulating test clusters",
	Long: `roachprod is a tool for manipulating test clusters, allowing easy starting,
stopping and wiping of clusters along with running load generators.
`,
}

var (
	numNodes       int
	username       string
	destroyAfter   time.Duration
	trackingFile   string
	extendLifetime time.Duration
	listDetails    bool
	clusterType    = "cockroach"
	secure         = false
	nodeEnv        = "COCKROACH_ENABLE_RPC_COMPRESSION=false"
	nodeArgs       []string
	external       = false
)

// This is a temporary hack to break package dependency cycles.
// It just calls the "real" ListCloud function and then initializes
// the local cluster.  In a follow-on patch, the local cluster
// will be just another type of CloudProvider, so we'll be able to
// get rid of this hack.
func listCloudAndLocal() (*cld.Cloud, error) {
	cloud, err := cld.ListCloud()
	if err != nil {
		return nil, err
	}

	// Initialize the local cluster (if it exists)
	if sc, ok := install.Clusters[config.Local]; ok {
		c := &cld.CloudCluster{
			Name:      config.Local,
			User:      config.OSUser.Username,
			CreatedAt: time.Now(),
			Lifetime:  time.Hour,
		}
		cloud.Clusters[config.Local] = c

		for range sc.VMs {
			c.VMs = append(c.VMs, cld.VM{
				Name:      "localhost",
				CreatedAt: c.CreatedAt,
				Lifetime:  time.Hour,
				PrivateIP: "127.0.0.1",
				PublicIP:  "127.0.0.1",
				Zone:      config.Local,
			})
		}
	}

	// Sort VMs for each cluster. We want to make sure we always have the same order.
	for _, c := range cloud.Clusters {
		sort.Sort(c.VMs)
	}
	return cloud, nil
}

func sortedClusters() []string {
	var r []string
	for n := range install.Clusters {
		r = append(r, n)
	}
	sort.Strings(r)
	return r
}

func newCluster(name string, reserveLoadGen bool) (*install.SyncedCluster, error) {
	nodeNames := "all"
	{
		parts := strings.Split(name, ":")
		switch len(parts) {
		case 2:
			nodeNames = parts[1]
			fallthrough
		case 1:
			name = parts[0]
		case 0:
			return nil, fmt.Errorf("no cluster specified")
		default:
			return nil, fmt.Errorf("invalid cluster name: %s", name)
		}
	}

	c, ok := install.Clusters[name]
	if !ok {
		return nil, fmt.Errorf(`unknown cluster: %s

Available clusters:
  %s

Hint: use "roachprod sync" to update the list of available clusters.
`,
			name, strings.Join(sortedClusters(), "\n  "))
	}

	switch clusterType {
	case "cockroach":
		c.Impl = install.Cockroach{}
	case "cassandra":
		c.Impl = install.Cassandra{}
	default:
		return nil, fmt.Errorf("unknown cluster type: %s", clusterType)
	}

	nodes, err := install.ListNodes(nodeNames, len(c.VMs))
	if err != nil {
		return nil, err
	}

	c.Nodes = nodes
	if reserveLoadGen {
		// TODO(marc): make loadgen node configurable. For now, we always use the
		// last ID (1-indexed).
		c.LoadGen = len(c.VMs)
	} else {
		c.LoadGen = -1
	}
	c.Secure = secure
	c.Env = nodeEnv
	c.Args = nodeArgs
	return c, nil
}

func verifyClusterName(clusterName string) (string, error) {
	if len(clusterName) == 0 {
		return "", fmt.Errorf("cluster name cannot be blank")
	}
	if clusterName == config.Local {
		return clusterName, nil
	}

	account := username
	if len(username) == 0 {
		var err error
		account, err = cld.FindActiveAccount()
		if err != nil {
			return "", err
		}
	}

	if !strings.HasPrefix(clusterName, account+"-") {
		i := strings.Index(clusterName, "-")
		suffix := clusterName
		if i != -1 {
			// The user specified a username prefix, but it didn't match the active
			// account name. For example, assuming the account is "peter", `roachprod
			// create joe-perf` should be specified as `roachprod create joe-perf -u
			// joe`.
			suffix = clusterName[i+1:]
		} else {
			// The user didn't specify a username prefix. For example, assuming the
			// account is "peter", `roachprod create perf` should be specified as
			// `roachprod create peter-perf`.
		}
		return "", fmt.Errorf("malformed cluster ID %s, did you mean %s-%s?",
			clusterName, account, suffix)
	}

	return clusterName, nil
}

var createVMOpts cld.VMOpts

var createCmd = &cobra.Command{
	Use:          "create <cluster id>",
	Short:        "create a cluster",
	Long:         ``,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("wrong number of arguments")
		}

		if numNodes <= 0 || numNodes >= 1000 {
			// Upper limit is just for safety.
			return fmt.Errorf("number of nodes must be in [1..999]")
		}

		clusterName, err := verifyClusterName(args[0])
		if err != nil {
			return err
		}
		fmt.Printf("Creating cluster %s with %d nodes\n", clusterName, numNodes)

		if clusterName != config.Local {
			cloud, err := listCloudAndLocal()
			if err != nil {
				return err
			}
			if _, ok := cloud.Clusters[clusterName]; ok {
				return fmt.Errorf("cluster %s already exists", clusterName)
			}
		} else {
			if _, ok := install.Clusters[clusterName]; ok {
				return fmt.Errorf("cluster %s already exists", clusterName)
			}
		}

		if err := cld.CreateCluster(clusterName, numNodes, createVMOpts); err != nil {
			return err
		}

		fmt.Println("OK")

		if clusterName != config.Local {
			{
				cloud, err := listCloudAndLocal()
				if err != nil {
					return err
				}

				c, ok := cloud.Clusters[clusterName]
				if !ok {
					return fmt.Errorf("could not find %s in list of cluster", clusterName)
				}
				c.PrintDetails()

				if err := syncAll(cloud); err != nil {
					return err
				}
			}

			{
				// Wait for the nodes in the cluster to start.
				install.Clusters = map[string]*install.SyncedCluster{}
				if err := loadClusters(); err != nil {
					return err
				}

				c, err := newCluster(clusterName, false)
				if err != nil {
					return err
				}

				if err := c.Wait(); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < numNodes; i++ {
				err := os.MkdirAll(fmt.Sprintf(os.ExpandEnv("${HOME}/local/%d"), i+1), 0755)
				if err != nil {
					return err
				}
			}
		}

		return nil
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

		clusterName, err := verifyClusterName(args[0])
		if err != nil {
			return err
		}

		if clusterName != config.Local {
			cloud, err := listCloudAndLocal()
			if err != nil {
				return err
			}

			c, ok := cloud.Clusters[clusterName]
			if !ok {
				return fmt.Errorf("cluster %s does not exist", clusterName)
			}

			fmt.Printf("Destroying cluster %s with %d nodes\n", clusterName, len(c.VMs))
			if err := cld.DestroyCluster(c); err != nil {
				return err
			}
		} else {
			if _, ok := install.Clusters[clusterName]; !ok {
				return fmt.Errorf("cluster %s does not exist", clusterName)
			}
			c, err := newCluster(clusterName, false /* reserveLoadGen */)
			if err != nil {
				return err
			}
			c.Wipe()
			for _, i := range c.Nodes {
				err := os.RemoveAll(fmt.Sprintf(os.ExpandEnv("${HOME}/local/%d"), i))
				if err != nil {
					return err
				}
			}
			if err := os.Remove(filepath.Join(os.ExpandEnv(config.DefaultHostDir), c.Name)); err != nil {
				return err
			}
		}

		fmt.Println("OK")
		return nil
	},
}

var listCmd = &cobra.Command{
	Use:   "list [--details]",
	Short: "retrieve the list of clusters",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		account, err := cld.FindActiveAccount()
		if err != nil {
			return err
		}
		fmt.Printf("Account: %s\n", account)

		cloud, err := listCloudAndLocal()
		if err != nil {
			return err
		}

		// Align columns left and separate with at least two spaces.
		tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
		for _, c := range cloud.Clusters {
			if listDetails {
				c.PrintDetails()
			} else {
				fmt.Fprintf(tw, "%s:\t%d", c.Name, len(c.VMs))
				if !c.IsLocal() {
					fmt.Fprintf(tw, "\t(%s)", c.LifetimeRemaining().Round(time.Second))
				} else {
					fmt.Fprintf(tw, "\t(-)")
				}
				fmt.Fprintf(tw, "\n")
			}
		}
		if err := tw.Flush(); err != nil {
			return err
		}

		// Optionally print any dangling instances with errors
		if listDetails {
			collated := cloud.BadInstanceErrors()

			// Sort by Error() value for stable output
			var errors ui.ErrorsByError
			for err := range collated {
				errors = append(errors, err)
			}
			sort.Sort(errors)

			for _, e := range errors {
				fmt.Printf("%s: %s\n", e, collated[e].Names())
			}
		}

		return syncAll(cloud)
	},
}

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "sync ssh keys/config and hosts files",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		cloud, err := listCloudAndLocal()
		if err != nil {
			return err
		}

		return syncAll(cloud)
	},
}

var lockFile = os.ExpandEnv("$HOME/.roachprod/LOCK")

var bashCompletion = os.ExpandEnv("$HOME/.roachprod/bash-completion.sh")

func syncAll(cloud *cld.Cloud) error {
	fmt.Println("Syncing...")

	// Acquire a filesystem lock so that two concurrent `roachprod sync`
	// operations don't clobber each other.
	f, err := os.Create(lockFile)
	if err != nil {
		return errors.Wrapf(err, "creating lock file %q", lockFile)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		return errors.Wrap(err, "acquiring lock on %q")
	}
	defer f.Close()

	if err := syncHosts(cloud); err != nil {
		return err
	}
	if err := cld.CleanSSH(); err != nil {
		return err
	}

	{
		names := make([]string, 0, len(cloud.Clusters))
		for name := range cloud.Clusters {
			names = append(names, name)
		}
		for _, cmd := range []*cobra.Command{
			destroyCmd, statusCmd, monitorCmd, startCmd, stopCmd, wipeCmd, sshCmd,
		} {
			cmd.ValidArgs = names
		}
		rootCmd.GenBashCompletionFile(bashCompletion)
	}
	return cld.ConfigSSH()
}

var gcCmd = &cobra.Command{
	Use:   "gc",
	Short: "GC expired clusters; sends email if properly configured\n",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		cloud, err := listCloudAndLocal()
		if err != nil {
			return err
		}

		return cld.GCClusters(cloud, trackingFile, destroyAfter)
	},
}

var extendCmd = &cobra.Command{
	Use:   "extend",
	Short: "extend the lifetime of the cluster by --lifetime amount of time",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("wrong number of arguments")
		}

		clusterName, err := verifyClusterName(args[0])
		if err != nil {
			return err
		}

		cloud, err := listCloudAndLocal()
		if err != nil {
			return err
		}

		c, ok := cloud.Clusters[clusterName]
		if !ok {
			return fmt.Errorf("cluster %s does not exist", clusterName)
		}

		if err := cld.ExtendCluster(c, extendLifetime); err != nil {
			return err
		}

		// Reload the clusters and print details.
		cloud, err = listCloudAndLocal()
		if err != nil {
			return err
		}

		c, ok = cloud.Clusters[clusterName]
		if !ok {
			return fmt.Errorf("cluster %s does not exist", clusterName)
		}

		c.PrintDetails()

		return nil
	},
}

var startCmd = &cobra.Command{
	Use:   "start <cluster>",
	Short: "start a cluster",
	Long:  `Start a cluster.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}
		c.Start()
		return nil
	},
}

var stopCmd = &cobra.Command{
	Use:   "stop <cluster>",
	Short: "stop a cluster",
	Long:  `Stop a cluster.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}
		c.Stop()
		return nil
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "retrieve the status of a cluster",
	Long:  `Retrieve the status of a cluster.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}
		c.Status()
		return nil
	},
}

var monitorCmd = &cobra.Command{
	Use:   "monitor",
	Short: "monitor the status of a cluster",
	Long:  `Continuously monitor the status of a cluster.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}
		for i := range c.Monitor() {
			fmt.Printf("%d: %s\n", i.Index, i.Msg)
		}
		return nil
	},
}

var wipeCmd = &cobra.Command{
	Use:   "wipe <cluster>",
	Short: "wipe a cluster",
	Long:  `Wipe a cluster.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}
		c.Wipe()
		return nil
	},
}

var runCmd = &cobra.Command{
	Use:   "run <cluster> <command> [args]",
	Short: "run a command on the nodes in a cluster",
	Long:  `Run a command on the nodes in a cluster.`,
	Args:  cobra.MinimumNArgs(2),
	RunE: func(_ *cobra.Command, args []string) error {
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}

		cmd := strings.TrimSpace(strings.Join(args[1:], " "))
		title := cmd
		if len(title) > 30 {
			title = title[:27] + "..."
		}

		_ = c.Run(os.Stdout, c.Nodes, title, cmd)
		return nil
	},
}

var testCmd = &cobra.Command{
	Use:   "test <cluster> <name>...",
	Short: "run one or more tests on a cluster",
	Long: `Run one or more tests on a cluster. The test <name> must be one of:

	` + strings.Join(allTests(), "\n\t") + `

Alternately, an interrupted test can be resumed by specifying the output
directory of a previous test. For example:

	roachperf test denim kv_0.cockroach-6151ae1

will restart the kv_0 test on denim using the cockroach binary with the build
tag 6151ae1.`,
	Args: cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		for _, arg := range args[1:] {
			if err := runTest(arg, args[0]); err != nil {
				return err
			}
		}
		return nil
	},
}

var installCmd = &cobra.Command{
	Use:   "install <cluster> <software>",
	Short: "install 3rd party software",
	Long: `Install third party software. Currently available installation options are:

  cassandra
  mongodb
  postgres
  tools (fio, iftop, perf)`,
	Args: cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}
		return install.Install(c, args[1:])
	},
}

var putCmd = &cobra.Command{
	Use:   "put <cluster> <src> [<dest>]",
	Short: "copy a local file to the nodes in a cluster",
	Long:  `Copy a local file to the nodes in a cluster.`,
	Args:  cobra.RangeArgs(2, 3),
	RunE: func(cmd *cobra.Command, args []string) error {
		src := args[1]
		dest := path.Base(src)
		if len(args) == 3 {
			dest = args[2]
		}
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}
		c.Put(src, dest)
		return nil
	},
}

var getCmd = &cobra.Command{
	Use:   "get <cluster> <src> [<dest>]",
	Short: "copy a remote file from the nodes in a cluster",
	Long: `Copy a remote file from the nodes in a cluster. If the file is retrieved from
multiple nodes the destination file name will be prefixed with the node number.`,
	Args: cobra.RangeArgs(2, 3),
	RunE: func(cmd *cobra.Command, args []string) error {
		src := args[1]
		dest := path.Base(src)
		if len(args) == 3 {
			dest = args[2]
		}
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}
		c.Get(src, dest)
		return nil
	},
}

var sshCmd = &cobra.Command{
	Use:          "ssh <cluster> [args]",
	Short:        "ssh to a node on a remote cluster",
	Long:         `SSH to a node on a remote cluster.`,
	Args:         cobra.MinimumNArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}

		return c.Ssh(args[1:])
	},
}

var sqlCmd = &cobra.Command{
	Use:          "sql <cluster> [args]",
	Short:        "run `cockroach sql` on a remote cluster",
	Long:         "Run `cockroach sql` on a remote cluster.",
	Args:         cobra.MinimumNArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}
		cockroach, ok := c.Impl.(install.Cockroach)
		if !ok {
			return errors.New("sql is only valid on cockroach clusters")
		}
		return cockroach.SQL(c, args[1:])
	},
}

var pgurlCmd = &cobra.Command{
	Use:   "pgurl <cluster>",
	Short: "generate pgurls for the nodes in a cluster\n",
	Long:  `Generate pgurls for the nodes in a cluster.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}
		nodes := c.ServerNodes()
		ips := make([]string, len(nodes))

		if external {
			for i := 0; i < len(nodes); i++ {
				ips[i] = c.VMs[nodes[i]-1]
			}
		} else {
			display := fmt.Sprintf("%s: retrieving IP addresses", c.Name)
			c.Parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
				var err error
				ips[i], err = c.GetInternalIP(nodes[i])
				return nil, err
			})
		}

		var urls []string
		for i, ip := range ips {
			urls = append(urls, c.Impl.NodeURL(c, ip, c.Impl.NodePort(c, nodes[i])))
		}
		fmt.Println(strings.Join(urls, " "))
		return nil
	},
}

var adminurlCmd = &cobra.Command{
	Use:   "adminurl <cluster>",
	Short: "generate admin UI URLs for the nodes in a cluster\n",
	Long:  `Generate admin UI URLs for the nodes in a cluster.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newCluster(args[0], false /* reserveLoadGen */)
		if err != nil {
			return err
		}

		for _, node := range c.ServerNodes() {
			ip := c.VMs[node-1]
			port := install.GetAdminUIPort(c.Impl.NodePort(c, node))
			fmt.Printf("http://%s:%d/\n", ip, port)
		}
		return nil
	},
}

var webCmd = &cobra.Command{
	Use:   "web <testdir> [<testdir>]",
	Short: "visualize and compare test output",
	Long: `
Visualize the output of a single test or compare the output of two tests.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return web(args)
	},
}

var dumpCmd = &cobra.Command{
	Use:   "dump <testdir> [<testdir>]",
	Short: "dump test output",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		return dump(args)
	},
}

func main() {
	cobra.EnableCommandSorting = false

	rootCmd.AddCommand(createCmd, destroyCmd, extendCmd, listCmd, syncCmd, gcCmd,
		statusCmd, monitorCmd, startCmd, stopCmd, runCmd, wipeCmd, testCmd,
		installCmd, putCmd, getCmd, sshCmd, pgurlCmd, adminurlCmd, sqlCmd, webCmd, dumpCmd)
	rootCmd.Flags().BoolVar(
		&ssh.InsecureIgnoreHostKey, "insecure-ignore-host-key", true, "don't check ssh host keys")

	for _, cmd := range []*cobra.Command{createCmd, destroyCmd, extendCmd} {
		cmd.Flags().StringVarP(&username, "username", "u", os.Getenv("ROACHPROD_USER"),
			"Username to run under, detect if blank")
	}

	createCmd.Flags().DurationVarP(&createVMOpts.Lifetime, "lifetime", "l", 12*time.Hour, "Lifetime of the cluster")
	createCmd.Flags().BoolVar(&createVMOpts.UseLocalSSD, "local-ssd", true, "Use local SSD")
	createCmd.Flags().StringVar(&createVMOpts.MachineType, "machine-type", "n1-standard-4", "Machine type (see https://cloud.google.com/compute/docs/machine-types)")
	createCmd.Flags().IntVarP(&numNodes, "nodes", "n", 4, "Number of nodes")
	createCmd.Flags().StringSliceVarP(&config.Zones, "zones", "z", []string{"us-east1-b", "us-west1-b", "europe-west2-b"}, "Zones for cluster")
	createCmd.Flags().BoolVar(&createVMOpts.GeoDistributed, "geo", false, "Create geo-distributed cluster")

	extendCmd.Flags().DurationVarP(&extendLifetime, "lifetime", "l", 12*time.Hour, "Lifetime of the cluster")

	listCmd.Flags().BoolVarP(&listDetails, "details", "d", false, "Show cluster details")

	gcCmd.Flags().StringVar(&config.GCEmailOpts.From, "email-from", "", "Address of the sender")
	gcCmd.Flags().StringVar(&config.GCEmailOpts.Host, "email-host", "", "SMTP host")
	gcCmd.Flags().IntVar(&config.GCEmailOpts.Port, "email-port", 587, "SMTP port")
	gcCmd.Flags().StringVar(&config.GCEmailOpts.User, "email-user", "", "SMTP user")
	gcCmd.Flags().StringVar(&config.GCEmailOpts.Password, "email-password", "", "SMTP password")
	gcCmd.Flags().DurationVar(&destroyAfter, "destroy-after", 6*time.Hour, "Destroy when this much time past expiration")
	gcCmd.Flags().StringVar(&trackingFile, "tracking-file", "roachprod.tracking.txt", "Tracking file to avoid duplicate emails")

	pgurlCmd.Flags().BoolVar(
		&external, "external", false, "return pgurls for external connections")

	sshCmd.Flags().BoolVar(
		&secure, "secure", false, "use a secure cluster")

	startCmd.Flags().StringVarP(
		&config.Binary, "binary", "b", "./cockroach", "the remote cockroach binary used to start a server")
	startCmd.Flags().BoolVar(
		&install.StartOpts.Sequential, "sequential", false, "start nodes sequentially so node IDs match hostnames")

	testCmd.Flags().StringVarP(
		&config.Binary, "binary", "b", "./cockroach", "the remote cockroach binary used to start a server")
	testCmd.Flags().DurationVarP(
		&duration, "duration", "d", 5*time.Minute, "the duration to run each test")
	testCmd.Flags().StringVarP(
		&concurrency, "concurrency", "c", "1-64", "the concurrency to run each test")

	for _, cmd := range []*cobra.Command{
		getCmd, putCmd, runCmd, startCmd, statusCmd, stopCmd, testCmd, wipeCmd, pgurlCmd, sqlCmd,
		installCmd,
	} {
		cmd.Flags().BoolVar(
			&secure, "secure", false, "use a secure cluster")
		cmd.Flags().StringSliceVarP(
			&nodeArgs, "args", "a", nil, "node arguments")
		cmd.Flags().StringVarP(
			&nodeEnv, "env", "e", nodeEnv, "node environment variables")
		cmd.Flags().StringVarP(
			&clusterType, "type", "t", clusterType, `cluster type ("cockroach" or "cassandra")`)

		if cmd.Long == "" {
			cmd.Long = cmd.Short
		}
		cmd.Long += fmt.Sprintf(`

By default the operation is performed on all nodes in <cluster>. A subset
of nodes can be specified by appending :<nodes> to the cluster name. The
syntax of <nodes> is a comma separated list of specific node IDs or range
of IDs. For example:

    roachperf %[1]s marc-test:1-3,8-9

will perform %[1]s on:

    marc-test-1
    marc-test-2
    marc-test-3
    marc-test-8
    marc-test-9
`, cmd.Name())
	}

	var err error
	config.OSUser, err = user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to lookup current user: %s\n", err)
		os.Exit(1)
	}

	if err := initHostDir(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	if err := loadClusters(); err != nil {
		// We don't want to exit as we may be looking at the help message.
		fmt.Printf("problem loading clusters: %s\n", err)
	}

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
