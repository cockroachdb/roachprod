package install

import (
	"fmt"
	"github.com/cockroachdb/roachprod/config"
	"github.com/cockroachdb/roachprod/ssh"
	"strings"
)

var StartOpts struct {
	Sequential bool
}

type Cockroach struct{}

func (r Cockroach) Start(c *SyncedCluster) {
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
		session, err := ssh.NewSSHSession(user, host)
		if err != nil {
			return nil, err
		}
		defer session.Close()

		port := r.NodePort(c, nodes[i])

		var args []string
		if c.Secure {
			args = append(args, "--certs-dir=certs")
		} else {
			args = append(args, "--insecure")
		}
		dir := "/mnt/data1/cockroach"
		logDir := "${HOME}/logs"
		if c.IsLocal() {
			dir = fmt.Sprintf("${HOME}/local/cockroach%d", nodes[i])
			logDir = fmt.Sprintf("${HOME}/local/cockroach%d/logs", nodes[i])
		}
		args = append(args, "--store=path="+dir)
		args = append(args, "--log-dir="+logDir)
		args = append(args, "--background")
		cache := 25
		if c.IsLocal() {
			cache /= len(nodes)
			if cache == 0 {
				cache = 1
			}
		}
		args = append(args, fmt.Sprintf("--cache=%d%%", cache))
		args = append(args, fmt.Sprintf("--max-sql-memory=%d%%", cache))
		args = append(args, fmt.Sprintf("--port=%d", port))
		args = append(args, fmt.Sprintf("--http-port=%d", port+1))
		if locality := c.locality(nodes[i]); locality != "" {
			args = append(args, "--locality="+locality)
		}
		if nodes[i] != 1 {
			args = append(args, fmt.Sprintf("--join=%s:%d", host1, r.NodePort(c, 1)))
		}
		args = append(args, c.Args...)
		cmd := "mkdir -p " + logDir + "; " +
			c.Env + " " + config.Binary + " start " + strings.Join(args, " ") +
			" >> " + logDir + "/cockroach.stdout 2>> " + logDir + "/cockroach.stderr"
		return session.CombinedOutput(cmd)
	})

	// Check to see if node 1 was started indicating the cluster was
	// bootstrapped.
	var bootstrapped bool
	for _, i := range nodes {
		if i == 1 {
			bootstrapped = true
			break
		}
	}

	if bootstrapped {
		var msg string
		display = fmt.Sprintf("%s: initializing cluster settings", c.Name)
		c.Parallel(display, 1, 0, func(i int) ([]byte, error) {
			session, err := ssh.NewSSHSession(c.user(1), c.host(1))
			if err != nil {
				return nil, err
			}
			defer session.Close()

			cmd := config.Binary + ` sql --url ` + r.NodeURL(c, "localhost", r.NodePort(c, 1)) + ` -e "
set cluster setting kv.allocator.stat_based_rebalancing.enabled = false;
set cluster setting server.remote_debugging.mode = 'any';
"`
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
