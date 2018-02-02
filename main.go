package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "roachprod [command] (flags)",
	Short: "roachprod tool for manipulating test clusters",
	Long: `roachprod is a tool for manipulating test clusters, allowing easy starting,
stopping and wiping of clusters along with running load generators.
`,
}

var (
	osUser         *user.User
	numNodes       int
	username       string
	destroyAfter   time.Duration
	trackingFile   string
	extendLifetime time.Duration
	listDetails    bool
	zones          []string
	clusterType    = "cockroach"
	secure         = false
	nodeEnv        = "COCKROACH_ENABLE_RPC_COMPRESSION=false"
	nodeArgs       []string
	binary         = "./cockroach"
	clusters       = map[string]*syncedCluster{}
)

func allNodes(total int) []int {
	r := make([]int, total)
	for i := range r {
		r[i] = i + 1
	}
	return r
}

func listNodes(s string, total int) ([]int, error) {
	if s == "all" {
		return allNodes(total), nil
	}

	m := map[int]bool{}
	for _, p := range strings.Split(s, ",") {
		parts := strings.Split(p, "-")
		switch len(parts) {
		case 1:
			i, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, err
			}
			m[i] = true
		case 2:
			from, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, err
			}
			to, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, err
			}
			for i := from; i <= to; i++ {
				m[i] = true
			}
		default:
			return nil, fmt.Errorf("unable to parse nodes specification: %s", p)
		}
	}

	r := make([]int, 0, len(m))
	for i := range m {
		r = append(r, i)
	}
	sort.Ints(r)
	return r, nil
}

func findLocalBinary() error {
	// For "local" clusters we have to find the binary to run and translate it to
	// an absolute path. First, look for the binary in PATH.
	path, err := exec.LookPath(binary)
	if err != nil {
		if strings.HasPrefix(binary, "/") {
			return err
		}
		// We're unable to find the binary in PATH and "binary" is a relative path:
		// look in the cockroach repo.
		gopath := os.Getenv("GOPATH")
		if gopath == "" {
			return err
		}
		path = gopath + "/src/github.com/cockroachdb/cockroach/" + binary
		var err2 error
		path, err2 = exec.LookPath(path)
		if err2 != nil {
			return err
		}
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}
	binary = path
	return nil
}

func sortedClusters() []string {
	var r []string
	for n := range clusters {
		r = append(r, n)
	}
	sort.Strings(r)
	return r
}

func newCluster(name string, reserveLoadGen bool) (*syncedCluster, error) {
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

	c, ok := clusters[name]
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
		c.impl = cockroach{}
	case "cassandra":
		c.impl = cassandra{}
	default:
		return nil, fmt.Errorf("unknown cluster type: %s", clusterType)
	}

	total := len(c.vms)
	if c.isLocal() {
		// If ${HOME}/local exists default to the number of nodes in the cluster.
		if entries, err := filepath.Glob(os.ExpandEnv("${HOME}/local/*")); err == nil {
			total = len(entries)
		}
		if total == 0 {
			total = 1
		}
	}

	nodes, err := listNodes(nodeNames, total)
	if err != nil {
		return nil, err
	}

	c.nodes = nodes
	if reserveLoadGen {
		// TODO(marc): make loadgen node configurable. For now, we always use the
		// last ID (1-indexed).
		c.loadGen = len(c.vms)
	} else {
		c.loadGen = -1
	}
	c.secure = secure
	c.env = nodeEnv
	c.args = nodeArgs

	if c.isLocal() {
		var max int
		for _, i := range c.nodes {
			if max < i {
				max = i
			}
		}

		c.vms = make([]string, max)
		c.users = make([]string, max)
		c.localities = make([]string, max)
		for i := range c.vms {
			c.vms[i] = "localhost"
			c.users[i] = osUser.Username
		}

		if err := findLocalBinary(); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func verifyClusterName(clusterName string) (string, error) {
	if len(clusterName) == 0 {
		return "", fmt.Errorf("cluster name cannot be blank")
	}

	account := username
	if len(username) == 0 {
		var err error
		account, err = findActiveAccount()
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

var createVMOpts VMOpts

var createCmd = &cobra.Command{
	Use:   "create <cluster id>",
	Short: "create a cluster",
	Long:  ``,
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

		{
			cloud, err = listCloud()
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
			clusters = map[string]*syncedCluster{}
			if err := loadClusters(); err != nil {
				return err
			}

			c, err := newCluster(clusterName, false)
			if err != nil {
				return err
			}

			if err := c.wait(); err != nil {
				return err
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
	Use:   "list [--details]",
	Short: "retrieve the list of clusters",
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

		// Align columns left and separate with at least two spaces.
		tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
		for _, c := range cloud.Clusters {
			if listDetails {
				c.PrintDetails()
			} else {
				fmt.Fprintf(tw, "%s:\t%d\t(%s)\n", c.Name, len(c.VMs), c.LifetimeRemaining().Round(time.Second))
			}
		}
		if err := tw.Flush(); err != nil {
			return err
		}

		if listDetails {
			if len(cloud.InvalidName) > 0 {
				fmt.Printf("Bad VM names: %s\n", strings.Join(cloud.InvalidName.Names(), " "))
			}
			if len(cloud.NoExpiration) > 0 {
				fmt.Printf("No expiration: %s\n", strings.Join(cloud.NoExpiration.Names(), " "))
			}
			if len(cloud.BadNetwork) > 0 {
				fmt.Printf("Bad network: %s\n", strings.Join(cloud.BadNetwork.Names(), " "))
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

var gcCmd = &cobra.Command{
	Use:   "gc",
	Short: "GC expired clusters; sends email if properly configured\n",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		cloud, err := listCloud()
		if err != nil {
			return err
		}

		return gcClusters(cloud, trackingFile, destroyAfter)
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

		cloud, err := listCloud()
		if err != nil {
			return err
		}

		c, ok := cloud.Clusters[clusterName]
		if !ok {
			return fmt.Errorf("cluster %s does not exist", clusterName)
		}

		if err := extendCluster(c, extendLifetime); err != nil {
			return err
		}

		// Reload the clusters and print details.
		cloud, err = listCloud()
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
		c.start()
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
		c.stop()
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
		c.status()
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
		for i := range c.monitor() {
			fmt.Printf("%d: %s\n", i.index, i.msg)
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
		c.wipe()
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

		_ = c.run(os.Stdout, c.nodes, title, cmd)
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

// TODO(dan): This is quite similar to testCmd. I'm leaving it separate while
// iterating, but once that dies down, see if it makes sense to fold this into
// testCmd.
var workloadTestCmd = &cobra.Command{
	Use:   "workload-test <cluster> <name> <workload-invocation>",
	Short: "run one or more tests on a cluster",
	Args:  cobra.MinimumNArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		clusterName, testName, workloadCmd := args[0], args[1], args[2:]
		workloadTest(clusterName, `` /* dir */, testName, strings.Join(workloadCmd, ` `))
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
		return install(c, args[1:])
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
		c.put(src, dest)
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
		c.get(src, dest)
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
		return c.ssh(args[1:])
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

		display := fmt.Sprintf("%s: retrieving IP addresses", c.name)
		nodes := c.serverNodes()
		ips := make([]string, len(nodes))
		c.parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
			var err error
			ips[i], err = c.getInternalIP(nodes[i])
			return nil, err
		})

		var urls []string
		for i, ip := range ips {
			urls = append(urls, c.impl.nodeURL(c, ip, c.impl.nodePort(c, nodes[i])))
		}
		fmt.Println(strings.Join(urls, " "))
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
		statusCmd, monitorCmd, startCmd, stopCmd, runCmd, wipeCmd, testCmd, workloadTestCmd,
		installCmd, putCmd, getCmd, sshCmd, pgurlCmd, webCmd, dumpCmd)
	rootCmd.Flags().BoolVar(
		&insecureIgnoreHostKey, "insecure-ignore-host-key", true, "don't check ssh host keys")

	createCmd.Flags().DurationVarP(&createVMOpts.Lifetime, "lifetime", "l", 12*time.Hour, "Lifetime of the cluster")
	createCmd.Flags().BoolVar(&createVMOpts.UseLocalSSD, "local-ssd", true, "Use local SSD")
	createCmd.Flags().StringVar(&createVMOpts.MachineType, "machine-type", "n1-standard-4", "Machine type (see https://cloud.google.com/compute/docs/machine-types)")
	createCmd.Flags().IntVarP(&numNodes, "nodes", "n", 4, "Number of nodes")
	createCmd.Flags().StringVarP(&username, "username", "u", "", "Username to run under, detect if blank")
	createCmd.Flags().StringSliceVarP(&zones, "zones", "z", []string{"us-east1-b", "us-west1-b", "europe-west2-b"}, "Zones for cluster")
	createCmd.Flags().BoolVar(&createVMOpts.GeoDistributed, "geo", false, "Create geo-distributed cluster")

	destroyCmd.Flags().StringVarP(&username, "username", "u", "", "Username to run under, detect if blank")

	extendCmd.Flags().DurationVarP(&extendLifetime, "lifetime", "l", 12*time.Hour, "Lifetime of the cluster")
	extendCmd.Flags().StringVarP(&username, "username", "u", "", "Username to run under, detect if blank")

	listCmd.Flags().BoolVarP(&listDetails, "details", "d", false, "Show cluster details")

	gcCmd.Flags().StringVar(&gcEmailOpts.From, "email-from", "", "Address of the sender")
	gcCmd.Flags().StringVar(&gcEmailOpts.Host, "email-host", "", "SMTP host")
	gcCmd.Flags().IntVar(&gcEmailOpts.Port, "email-port", 587, "SMTP port")
	gcCmd.Flags().StringVar(&gcEmailOpts.User, "email-user", "", "SMTP user")
	gcCmd.Flags().StringVar(&gcEmailOpts.Password, "email-password", "", "SMTP password")
	gcCmd.Flags().DurationVar(&destroyAfter, "destroy-after", 6*time.Hour, "Destroy when this much time past expiration")
	gcCmd.Flags().StringVar(&trackingFile, "tracking-file", "roachprod.tracking.txt", "Tracking file to avoid duplicate emails")

	sshCmd.Flags().BoolVar(
		&secure, "secure", false, "use a secure cluster")

	startCmd.Flags().StringVarP(
		&binary, "binary", "b", "./cockroach", "the remote cockroach binary used to start a server")

	testCmd.Flags().StringVarP(
		&binary, "binary", "b", "./cockroach", "the remote cockroach binary used to start a server")
	testCmd.Flags().DurationVarP(
		&duration, "duration", "d", 5*time.Minute, "the duration to run each test")
	testCmd.Flags().StringVarP(
		&concurrency, "concurrency", "c", "1-64", "the concurrency to run each test")

	for _, cmd := range []*cobra.Command{
		getCmd, putCmd, runCmd, startCmd, statusCmd, stopCmd, testCmd, wipeCmd, pgurlCmd, installCmd,
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
	osUser, err = user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to lookup current user: %s", err)
		os.Exit(1)
	}

	if err := loadClusters(); err != nil {
		// We don't want to exit as we may be looking at the help message.
		fmt.Printf("problem loading clusters: %s", err)
	}

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
