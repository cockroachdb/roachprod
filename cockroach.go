package main

import (
	"fmt"
	"strings"
)

type cockroach struct{}

func (r cockroach) start(c *syncedCluster) {
	display := fmt.Sprintf("%s: starting", c.name)
	host1 := c.host(1)
	nodes := c.serverNodes()
	c.parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		host := c.host(nodes[i])
		user := c.user(nodes[i])
		session, err := newSSHSession(user, host)
		if err != nil {
			return nil, err
		}
		defer session.Close()

		port := r.nodePort(c, nodes[i])

		var args []string
		if c.secure {
			args = append(args, "--certs-dir=certs")
		} else {
			args = append(args, "--insecure")
		}
		dir := "/mnt/data1/cockroach"
		if c.isLocal() {
			dir = fmt.Sprintf("${HOME}/local/cockroach%d", nodes[i])
		}
		args = append(args, "--store=path="+dir)
		args = append(args, "--logtostderr")
		args = append(args, "--log-dir=")
		args = append(args, "--background")
		cache := 25
		if c.isLocal() {
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
			args = append(args, fmt.Sprintf("--join=%s:%d", host1, r.nodePort(c, 1)))
		}
		args = append(args, c.args...)
		cmd := "mkdir -p " + dir + "/logs; " +
			c.env + " " + binary + " start " + strings.Join(args, " ") +
			" > " + dir + "/logs/cockroach.stdout 2> " + dir + "/logs/cockroach.stderr"
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
		display = fmt.Sprintf("%s: initializing cluster settings", c.name)
		c.parallel(display, 1, 0, func(i int) ([]byte, error) {
			session, err := newSSHSession(c.user(1), c.host(1))
			if err != nil {
				return nil, err
			}
			defer session.Close()

			cmd := binary + ` sql --url ` + r.nodeURL(c, "localhost", r.nodePort(c, 1)) + ` -e "
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

func (cockroach) nodeURL(c *syncedCluster, host string, port int) string {
	url := fmt.Sprintf("'postgres://root@%s:%d", host, port)
	if c.secure {
		url += "?sslcert=certs%2Fnode.crt&sslkey=certs%2Fnode.key&" +
			"sslrootcert=certs%2Fca.crt&sslmode=verify-full"
	} else {
		url += "?sslmode=disable"
	}
	url += "'"
	return url
}

func (cockroach) nodePort(c *syncedCluster, index int) int {
	const basePort = 26257
	port := basePort
	if c.isLocal() {
		port += (index - 1) * 2
	}
	return port
}
