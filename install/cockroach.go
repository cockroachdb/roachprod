package install

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/roachprod/config"
	"github.com/cockroachdb/roachprod/ssh"
	"github.com/hashicorp/go-version"
)

var StartOpts struct {
	Sequential bool
}

type Cockroach struct{}

func cockroachNodeBinary(c *SyncedCluster, i int) string {
	if !c.IsLocal() || filepath.IsAbs(config.Binary) {
		return config.Binary
	}

	path := filepath.Join(fmt.Sprintf(os.ExpandEnv("${HOME}/local/%d"), i), config.Binary)
	if _, err := os.Stat(path); err == nil {
		return path
	}

	// For "local" clusters we have to find the binary to run and translate it to
	// an absolute path. First, look for the binary in PATH.
	path, err := exec.LookPath(config.Binary)
	if err != nil {
		if strings.HasPrefix(config.Binary, "/") {
			return config.Binary
		}
		// We're unable to find the binary in PATH and "binary" is a relative path:
		// look in the cockroach repo.
		gopath := os.Getenv("GOPATH")
		if gopath == "" {
			return config.Binary
		}
		path = gopath + "/src/github.com/cockroachdb/cockroach/" + config.Binary
		var err2 error
		path, err2 = exec.LookPath(path)
		if err2 != nil {
			return config.Binary
		}
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return config.Binary
	}
	return path
}

func getCockroachVersion(c *SyncedCluster, i int, host, user string) (*version.Version, error) {
	session, err := ssh.NewSSHSession(user, host)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	cmd := cockroachNodeBinary(c, i) + " version"
	out, err := session.CombinedOutput(cmd)
	if err != nil {
		return nil, err
	}

	matches := regexp.MustCompile(`(?m)^Build Tag:\s+(.*)$`).FindSubmatch(out)
	if len(matches) != 2 {
		return nil, fmt.Errorf("unable to parse cockroach version output:%s", out)
	}

	version, err := version.NewVersion(string(matches[1]))
	if err != nil {
		return nil, err
	}
	return version, nil
}

func GetAdminUIPort(connPort int) int {
	return connPort + 1
}

func (r Cockroach) Start(c *SyncedCluster, extraArgs []string) {
	// Check to see if node 1 was started indicating the cluster was
	// bootstrapped.
	var bootstrapped bool
	for _, i := range c.ServerNodes() {
		if i == 1 {
			bootstrapped = true
			break
		}
	}

	if c.Secure && bootstrapped {
		dir := ""
		if c.IsLocal() {
			dir = `${HOME}/local/1`
		}

		// Check to see if the certs have already been initialized.
		var existsErr error
		display := fmt.Sprintf("%s: checking certs", c.Name)
		c.Parallel(display, 1, 0, func(i int) ([]byte, error) {
			session, err := ssh.NewSSHSession(c.user(1), c.host(1))
			if err != nil {
				return nil, err
			}
			defer session.Close()
			_, existsErr = session.CombinedOutput(`test -e ` + filepath.Join(dir, `certs.tar`))
			return nil, nil
		})

		if existsErr != nil {
			// Gather the internal IP addresses for every node in the cluster, even
			// if it won't be added to the cluster itself we still add the IP address
			// to the node cert.
			var msg string
			display := fmt.Sprintf("%s: initializing certs", c.Name)
			nodes := allNodes(len(c.VMs))
			var ips []string
			if !c.IsLocal() {
				ips = make([]string, len(nodes))
				c.Parallel("", len(nodes), 0, func(i int) ([]byte, error) {
					var err error
					ips[i], err = c.GetInternalIP(nodes[i])
					return nil, err
				})
			}

			// Generate the ca, client and node certificates on the first node.
			c.Parallel(display, 1, 0, func(i int) ([]byte, error) {
				session, err := ssh.NewSSHSession(c.user(1), c.host(1))
				if err != nil {
					return nil, err
				}
				defer session.Close()

				var nodeNames []string
				if c.IsLocal() {
					// For local clusters, we only need to add one of the VM IP addresses.
					nodeNames = append(nodeNames, "$(hostname)", c.VMs[0])
				} else {
					// Add both the local and external IP addresses, as well as the
					// hostnames to the node certificate.
					nodeNames = append(nodeNames, ips...)
					nodeNames = append(nodeNames, c.VMs...)
					for i := range c.VMs {
						nodeNames = append(nodeNames, fmt.Sprintf("%s-%04d", c.Name, i+1))
					}
				}

				var cmd string
				if c.IsLocal() {
					cmd = `cd ${HOME}/local/1 ; `
				}
				cmd += fmt.Sprintf(`
rm -fr certs
mkdir -p certs
%[1]s cert create-ca --certs-dir=certs --ca-key=certs/ca.key
%[1]s cert create-client root --certs-dir=certs --ca-key=certs/ca.key
%[1]s cert create-node localhost %[2]s --certs-dir=certs --ca-key=certs/ca.key
tar cvf certs.tar certs
`, cockroachNodeBinary(c, 1), strings.Join(nodeNames, " "))
				if _, err := session.CombinedOutput(cmd); err != nil {
					msg = err.Error()
				}
				return nil, nil
			})

			if msg != "" {
				fmt.Fprintln(os.Stderr, msg)
				os.Exit(1)
			}

			// Retrieve the certs.tar that was created on the first node.
			tmpfile, err := ioutil.TempFile("", "certs")
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			_ = tmpfile.Close()
			defer os.Remove(tmpfile.Name()) // clean up

			if err := func() error {
				session, err := ssh.NewSSHSession(c.user(1), c.host(1))
				if err != nil {
					return err
				}
				defer session.Close()
				return ssh.SCPGet(filepath.Join(dir, "certs.tar"),
					tmpfile.Name(), func(float64) {}, session)
			}(); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			// Read the certs.tar file we just downloaded. We'll be piping it to the
			// other nodes in the cluster.
			certsTar, err := ioutil.ReadFile(tmpfile.Name())
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			// Skip the the first node which is where we generated the certs.
			nodes = nodes[1:]
			c.Parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
				session, err := ssh.NewSSHSession(c.user(nodes[i]), c.host(nodes[i]))
				if err != nil {
					return nil, err
				}
				defer session.Close()

				session.Stdin = bytes.NewReader(certsTar)
				var cmd string
				if c.IsLocal() {
					cmd = fmt.Sprintf(`cd ${HOME}/local/%d ; `, nodes[i])
				}
				cmd += `tar xf -`
				_, err = session.CombinedOutput(cmd)
				return nil, err
			})
		}
	}

	display := fmt.Sprintf("%s: starting", c.Name)
	host1 := c.host(1)
	nodes := c.ServerNodes()

	p := 0
	if StartOpts.Sequential {
		p = 1
	}
	c.Parallel(display, len(nodes), p, func(i int) ([]byte, error) {
		host := c.host(nodes[i])
		user := c.user(nodes[i])

		vers, err := getCockroachVersion(c, nodes[i], host, user)
		if err != nil {
			return nil, err
		}

		session, err := ssh.NewSSHSession(user, host)
		if err != nil {
			return nil, err
		}
		defer session.Close()

		port := r.NodePort(c, nodes[i])

		var args []string
		if c.Secure {
			if c.IsLocal() {
				args = append(args, fmt.Sprintf("--certs-dir=${HOME}/local/%d/certs", nodes[i]))
			} else {
				args = append(args, "--certs-dir=certs")
			}
		} else {
			args = append(args, "--insecure")
		}
		dir := "/mnt/data1/cockroach"
		logDir := "${HOME}/logs"
		if c.IsLocal() {
			dir = fmt.Sprintf("${HOME}/local/%d/data", nodes[i])
			logDir = fmt.Sprintf("${HOME}/local/%d/logs", nodes[i])
		}
		args = append(args, "--store=path="+dir)
		args = append(args, "--log-dir="+logDir)
		args = append(args, "--background")
		if VersionSatifies(vers, ">=1.1") {
			cache := 25
			if c.IsLocal() {
				cache /= len(nodes)
				if cache == 0 {
					cache = 1
				}
			}
			args = append(args, fmt.Sprintf("--cache=%d%%", cache))
			args = append(args, fmt.Sprintf("--max-sql-memory=%d%%", cache))
		}
		args = append(args, fmt.Sprintf("--port=%d", port))
		args = append(args, fmt.Sprintf("--http-port=%d", GetAdminUIPort(port)))
		if locality := c.locality(nodes[i]); locality != "" {
			args = append(args, "--locality="+locality)
		}
		if nodes[i] != 1 {
			args = append(args, fmt.Sprintf("--join=%s:%d", host1, r.NodePort(c, 1)))
		}
		args = append(args, extraArgs...)

		binary := cockroachNodeBinary(c, nodes[i])
		// NB: this is awkward as when the process fails, the test runner will show an
		// unhelpful empty error (since everything has been redirected away). This is
		// unfortunately equally awkward to address.
		cmd := "mkdir -p " + logDir + "; " + c.Env +
			fmt.Sprintf(" ROACHPROD=%d ", nodes[i]) +
			binary + " start " + strings.Join(args, " ") +
			" >> " + logDir + "/cockroach.stdout 2>> " + logDir + "/cockroach.stderr"
		return session.CombinedOutput(cmd)
	})

	if bootstrapped {
		license := os.Getenv("COCKROACH_DEV_LICENSE")
		if license == "" {
			fmt.Printf("%s: COCKROACH_DEV_LICENSE unset: enterprise features will be unavailable\n",
				c.Name)
		}

		var msg string
		display = fmt.Sprintf("%s: initializing cluster settings", c.Name)
		c.Parallel(display, 1, 0, func(i int) ([]byte, error) {
			session, err := ssh.NewSSHSession(c.user(1), c.host(1))
			if err != nil {
				return nil, err
			}
			defer session.Close()

			var cmd string
			if c.IsLocal() {
				cmd = `cd ${HOME}/local/1 ; `
			}
			cmd += cockroachNodeBinary(c, 1) + " sql --url " +
				r.NodeURL(c, "localhost", r.NodePort(c, 1)) + " -e " +
				fmt.Sprintf(`"
SET CLUSTER SETTING server.remote_debugging.mode = 'any';
SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing';
SET CLUSTER SETTING enterprise.license = '%s';"`, license)
			out, err := session.CombinedOutput(cmd)
			if err != nil {
				msg = err.Error()
			} else {
				msg = strings.TrimSpace(string(out))
			}
			return nil, nil
		})

		fmt.Println(msg)
	}
}

func (Cockroach) NodeDir(c *SyncedCluster, index int) string {
	if c.IsLocal() {
		return os.ExpandEnv(fmt.Sprintf("${HOME}/local/%d/data", index))
	}
	return "/mnt/data1/cockroach"
}

func (Cockroach) NodeURL(c *SyncedCluster, host string, port int) string {
	url := fmt.Sprintf("'postgres://root@%s:%d", host, port)
	if c.Secure {
		url += "?sslcert=certs%2Fnode.crt&sslkey=certs%2Fnode.key&" +
			"sslrootcert=certs%2Fca.crt&sslmode=verify-full"
	} else {
		url += "?sslmode=disable"
	}
	url += "'"
	return url
}

func (Cockroach) NodePort(c *SyncedCluster, index int) int {
	const basePort = 26257
	port := basePort
	if c.IsLocal() {
		port += (index - 1) * 2
	}
	return port
}

func (r Cockroach) SQL(c *SyncedCluster, args []string) error {
	if len(args) == 0 || len(c.Nodes) == 1 {
		// If no arguments, we're going to get an interactive SQL shell. Require
		// exactly one target and ask SSH to provide a pseudoterminal.
		if len(args) == 0 && len(c.Nodes) != 1 {
			return fmt.Errorf("invalid number of nodes for interactive sql: %d", len(c.Nodes))
		}
		url := r.NodeURL(c, "localhost", r.NodePort(c, c.Nodes[0]))
		binary := cockroachNodeBinary(c, c.Nodes[0])
		allArgs := []string{binary, "sql", "--url", url}
		allArgs = append(allArgs, ssh.Escape(args))
		return c.Ssh([]string{"-t"}, allArgs)
	}

	// Otherwise, assume the user provided the "-e" flag, so we can reasonably
	// execute the query on all specified nodes.
	type result struct {
		node   int
		output string
	}
	resultChan := make(chan result, len(c.Nodes))

	display := fmt.Sprintf("%s: executing sql", c.Name)
	c.Parallel(display, len(c.Nodes), 0, func(i int) ([]byte, error) {
		session, err := ssh.NewSSHSession(c.user(c.Nodes[i]), c.host(c.Nodes[i]))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		var cmd string
		if c.IsLocal() {
			cmd = fmt.Sprintf(`cd ${HOME}/local/%d ; `, c.Nodes[i])
		}
		cmd += cockroachNodeBinary(c, c.Nodes[i]) + " sql --url " +
			r.NodeURL(c, "localhost", r.NodePort(c, c.Nodes[i])) + " " +
			ssh.Escape(args)

		out, err := session.CombinedOutput(cmd)
		if err != nil {
			resultChan <- result{node: c.Nodes[i], output: fmt.Sprintf("err=%s,out=%s", err, out)}
			return out, err
		}

		resultChan <- result{node: c.Nodes[i], output: string(out)}
		return nil, nil
	})

	results := make([]result, 0, len(c.Nodes))
	for _ = range c.Nodes {
		results = append(results, <-resultChan)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].node < results[j].node
	})
	for _, r := range results {
		fmt.Printf("node %d:\n%s", r.node, r.output)
	}

	return nil
}
