package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type clusterImpl interface {
	start(c *syncedCluster)
	nodeURL(c *syncedCluster, host string, port int) string
	nodePort(c *syncedCluster, index int) int
}

// A syncedCluster is created from the information in the synced hosts file.
//
// TODO(benesch): unify with CloudCluster.
type syncedCluster struct {
	// name, vms, users, localities are populated at init time.
	name       string
	vms        []string
	users      []string
	localities []string
	// all other fields are populated in newCluster.
	nodes   []int
	loadGen int
	secure  bool
	env     string
	args    []string
	impl    clusterImpl
}

func (c *syncedCluster) host(index int) string {
	return c.vms[index-1]
}

func (c *syncedCluster) user(index int) string {
	return c.users[index-1]
}

func (c *syncedCluster) locality(index int) string {
	return c.localities[index-1]
}

func (c *syncedCluster) isLocal() bool {
	return c.name == local
}

func (c *syncedCluster) serverNodes() []int {
	if c.loadGen == -1 {
		return c.nodes
	}
	newNodes := make([]int, 0, len(c.nodes))
	for _, i := range c.nodes {
		if i != c.loadGen {
			newNodes = append(newNodes, i)
		}
	}
	return newNodes
}

// getInternalIP returns the internal IP address of the specified node.
func (c *syncedCluster) getInternalIP(index int) (string, error) {
	if c.isLocal() {
		return c.host(index), nil
	}

	session, err := newSSHSession(c.user(index), c.host(index))
	if err != nil {
		return "", nil
	}
	defer session.Close()

	cmd := `hostname --all-ip-addresses`
	out, err := session.CombinedOutput(cmd)
	if err != nil {
		return "", nil
	}
	return strings.TrimSpace(string(out)), nil
}

func (c *syncedCluster) start() {
	c.impl.start(c)
}

func (c *syncedCluster) stop() {
	display := fmt.Sprintf("%s: stopping", c.name)
	c.parallel(display, len(c.nodes), 0, func(i int) ([]byte, error) {
		session, err := newSSHSession(c.user(c.nodes[i]), c.host(c.nodes[i]))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		cmd := `pkill -9 "cockroach|java|mongo|kv|ycsb" || true ;
`
		cmd += fmt.Sprintf("kill -9 $(lsof -t -i :%d -i :%d) 2>/dev/null || true ;\n",
			cockroach{}.nodePort(c, c.nodes[i]),
			cassandra{}.nodePort(c, c.nodes[i]))
		return session.CombinedOutput(cmd)
	})
}

func (c *syncedCluster) wipe() {
	display := fmt.Sprintf("%s: wiping", c.name)
	c.parallel(display, len(c.nodes), 0, func(i int) ([]byte, error) {
		session, err := newSSHSession(c.user(c.nodes[i]), c.host(c.nodes[i]))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		cmd := `pkill -9 "cockroach|java|mongo|kv|ycsb" || true ;
`
		cmd += fmt.Sprintf("kill -9 $(lsof -t -i :%d -i :%d) 2>/dev/null || true ;\n",
			cockroach{}.nodePort(c, c.nodes[i]),
			cassandra{}.nodePort(c, c.nodes[i]))
		if c.isLocal() {
			cmd += `rm -fr ${HOME}/local ;`
		} else {
			cmd += `find /mnt/data* -maxdepth 1 -type f -exec rm -f {} \; ;
rm -fr /mnt/data*/{auxiliary,local,tmp,cassandra,cockroach,cockroach-temp*,mongo-data} \; ;
`
		}
		return session.CombinedOutput(cmd)
	})
}

func (c *syncedCluster) status() {
	display := fmt.Sprintf("%s: status", c.name)
	results := make([]string, len(c.nodes))
	c.parallel(display, len(c.nodes), 0, func(i int) ([]byte, error) {
		session, err := newSSHSession(c.user(c.nodes[i]), c.host(c.nodes[i]))
		if err != nil {
			results[i] = err.Error()
			return nil, nil
		}
		defer session.Close()

		cmd := fmt.Sprintf("out=$(lsof -i :%d -i :%d -sTCP:LISTEN",
			cockroach{}.nodePort(c, c.nodes[i]),
			cassandra{}.nodePort(c, c.nodes[i]))
		cmd += ` | awk '!/COMMAND/ {print $1, $2}' | sort | uniq);
vers=$(` + binary + ` version 2>/dev/null | awk '/Build Tag:/ {print $NF}')
if [ -n "${out}" -a -n "${vers}" ]; then
  echo ${out} | sed "s/cockroach/cockroach-${vers}/g"
else
  echo ${out}
fi
`
		out, err := session.CombinedOutput(cmd)
		var msg string
		if err != nil {
			msg = err.Error()
		} else {
			msg = strings.TrimSpace(string(out))
			if msg == "" {
				msg = "not running"
			}
		}
		results[i] = msg
		return nil, nil
	})

	for i, r := range results {
		fmt.Printf("  %2d: %s\n", c.nodes[i], r)
	}
}

type nodeMonitorInfo struct {
	index int
	msg   string
}

func (c *syncedCluster) monitor() chan nodeMonitorInfo {
	ch := make(chan nodeMonitorInfo)
	nodes := c.serverNodes()

	for i := range nodes {
		go func(i int) {
			session, err := newSSHSession(c.user(nodes[i]), c.host(nodes[i]))
			if err != nil {
				ch <- nodeMonitorInfo{nodes[i], err.Error()}
				return
			}
			defer session.Close()

			go func() {
				p, err := session.StdoutPipe()
				if err != nil {
					ch <- nodeMonitorInfo{nodes[i], err.Error()}
					return
				}
				r := bufio.NewReader(p)
				for {
					line, _, err := r.ReadLine()
					if err == io.EOF {
						return
					}
					ch <- nodeMonitorInfo{nodes[i], string(line)}
				}
			}()

			// On each monitored node, we loop looking for a cockroach process. In
			// order to avoid polling with lsof, if we find a live process we use nc
			// (netcat) to connect to the rpc port which will block until the server
			// either decides to kill the connection or the process is killed.
			cmd := fmt.Sprintf(`
lastpid=0
while :; do
  pid=$(lsof -i :%[1]d -sTCP:LISTEN | awk '!/COMMAND/ {print $2}')
  if [ "${pid}" != "${lastpid}" ]; then
    if [ -n "${lastpid}" -a -z "${pid}" ]; then
      echo dead
    fi
    lastpid=${pid}
    if [ -n "${pid}" ]; then
      echo ${pid}
    fi
  fi

  if [ -n "${lastpid}" ]; then
    nc localhost %[1]d >/dev/null 2>&1
  else
    sleep 1
  fi
done
`,
				cockroach{}.nodePort(c, nodes[i]))

			if err := session.Run(cmd); err != nil {
				ch <- nodeMonitorInfo{nodes[i], err.Error()}
				return
			}
		}(i)
	}

	return ch
}

func (c *syncedCluster) run(w io.Writer, nodes []int, title, cmd string) error {
	display := fmt.Sprintf("%s: %s", c.name, title)
	errors := make([]error, len(nodes))
	results := make([]string, len(nodes))
	c.parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		session, err := newSSHSession(c.user(nodes[i]), c.host(nodes[i]))
		if err != nil {
			errors[i] = err
			results[i] = err.Error()
			return nil, nil
		}
		defer session.Close()

		out, err := session.CombinedOutput(cmd)
		msg := strings.TrimSpace(string(out))
		if err != nil {
			errors[i] = err
			msg += fmt.Sprintf("\n%v", err)
		}
		results[i] = msg
		return nil, nil
	})

	for i, r := range results {
		fmt.Fprintf(w, "  %2d: %s\n", nodes[i], r)
	}

	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO(peter): add a timeout to wait so we don't spin here indefinitely if
// there is a problem with one of the nodes. We only need to do this if we see
// problems here when creating clusters.
func (c *syncedCluster) wait() error {
	display := fmt.Sprintf("%s: waiting for nodes to start", c.name)
	errors := make([]error, len(c.nodes))
	c.parallel(display, len(c.nodes), 0, func(i int) ([]byte, error) {
		for {
			session, err := newSSHSession(c.user(c.nodes[i]), c.host(c.nodes[i]))
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			defer session.Close()

			if _, err := session.CombinedOutput("echo OK"); err != nil {
				errors[i] = err
			}
			return nil, nil
		}
	})

	for i, err := range errors {
		if err != nil {
			fmt.Printf("  %2d: %v\n", c.nodes[i], err)
			return err
		}
	}
	return nil
}

func (c *syncedCluster) cockroachVersions() map[string]int {
	sha := make(map[string]int)
	var mu sync.Mutex

	display := fmt.Sprintf("%s: cockroach version", c.name)
	nodes := c.serverNodes()
	c.parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		session, err := newSSHSession(c.user(c.nodes[i]), c.host(nodes[i]))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		cmd := binary + " version | awk '/Build Tag:/ {print $NF}'"
		out, err := session.CombinedOutput(cmd)
		var s string
		if err != nil {
			s = err.Error()
		} else {
			s = strings.TrimSpace(string(out))
		}
		mu.Lock()
		sha[s]++
		mu.Unlock()
		return nil, err
	})

	return sha
}

func (c *syncedCluster) runLoad(cmd string, stdout, stderr io.Writer) error {
	if c.loadGen == 0 {
		log.Fatalf("%s: no load generator node specified", c.name)
	}

	display := fmt.Sprintf("%s: retrieving IP addresses", c.name)
	nodes := c.serverNodes()
	ips := make([]string, len(nodes))
	c.parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		var err error
		ips[i], err = c.getInternalIP(nodes[i])
		return nil, err
	})

	session, err := newSSHSession(c.user(c.loadGen), c.host(c.loadGen))
	if err != nil {
		return err
	}
	defer session.Close()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer func() {
		signal.Stop(ch)
		close(ch)
	}()
	go func() {
		_, ok := <-ch
		if ok {
			c.stopLoad()
		}
	}()

	session.Stdout = stdout
	session.Stderr = stderr
	fmt.Fprintln(stdout, cmd)

	var urls []string
	for i, ip := range ips {
		urls = append(urls, c.impl.nodeURL(c, ip, c.impl.nodePort(c, nodes[i])))
	}
	return session.Run("ulimit -n 16384; " + cmd + " " + strings.Join(urls, " "))
}

const progressDone = "=======================================>"
const progressTodo = "----------------------------------------"

func formatProgress(p float64) string {
	i := int(math.Ceil(float64(len(progressDone)) * (1 - p)))
	return fmt.Sprintf("[%s%s] %.0f%%", progressDone[i:], progressTodo[:i], 100*p)
}

func (c *syncedCluster) put(src, dest string) {
	// TODO(peter): Only put 10 nodes at a time. When a node completes, output a
	// line indicating that.
	fmt.Printf("%s: putting %s %s\n", c.name, src, dest)

	type result struct {
		index int
		err   error
	}

	var writer uiWriter
	results := make(chan result, len(c.nodes))
	lines := make([]string, len(c.nodes))
	var linesMu sync.Mutex

	var wg sync.WaitGroup
	for i := range c.nodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			session, err := newSSHSession(c.user(c.nodes[i]), c.host(c.nodes[i]))
			if err == nil {
				defer session.Close()
				err = scpPut(src, dest, func(p float64) {
					linesMu.Lock()
					defer linesMu.Unlock()
					lines[i] = formatProgress(p)
				}, session)
			}
			results <- result{i, err}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var ticker *time.Ticker
	if isStdoutTerminal {
		ticker = time.NewTicker(100 * time.Millisecond)
	} else {
		ticker = time.NewTicker(1000 * time.Millisecond)
	}
	defer ticker.Stop()
	haveErr := false

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
			if !isStdoutTerminal {
				fmt.Printf(".")
			}
		case r, ok := <-results:
			done = !ok
			if ok {
				linesMu.Lock()
				if r.err != nil {
					haveErr = true
					lines[r.index] = r.err.Error()
				} else {
					lines[r.index] = "done"
				}
				linesMu.Unlock()
			}
		}
		if isStdoutTerminal {
			linesMu.Lock()
			for i := range lines {
				fmt.Fprintf(&writer, "  %2d: ", c.nodes[i])
				if lines[i] != "" {
					fmt.Fprintf(&writer, "%s", lines[i])
				} else {
					fmt.Fprintf(&writer, "%s", spinner[spinnerIdx%len(spinner)])
				}
				fmt.Fprintf(&writer, "\n")
			}
			linesMu.Unlock()
			writer.Flush(os.Stdout)
			spinnerIdx++
		}
	}

	if !isStdoutTerminal {
		fmt.Printf("\n")
		linesMu.Lock()
		for i := range lines {
			fmt.Printf("  %2d: %s\n", c.nodes[i], lines[i])
		}
		linesMu.Unlock()
	}

	if haveErr {
		log.Fatal("failed")
	}
}

func (c *syncedCluster) get(src, dest string) {
	// TODO(peter): Only get 10 nodes at a time. When a node completes, output a
	// line indicating that.
	fmt.Printf("%s: getting %s %s\n", c.name, src, dest)

	type result struct {
		index int
		err   error
	}

	var writer uiWriter
	results := make(chan result, len(c.nodes))
	lines := make([]string, len(c.nodes))
	var linesMu sync.Mutex

	var wg sync.WaitGroup
	for i := range c.nodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			session, err := newSSHSession(c.user(c.nodes[i]), c.host(c.nodes[i]))
			if err == nil {
				defer session.Close()
				dest := dest
				if len(c.nodes) > 1 {
					dest = fmt.Sprintf("%d.%s", c.nodes[i], dest)
				}
				err = scpGet(src, dest, func(p float64) {
					linesMu.Lock()
					defer linesMu.Unlock()
					lines[i] = formatProgress(p)
				}, session)
			}
			results <- result{i, err}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var ticker *time.Ticker
	if isStdoutTerminal {
		ticker = time.NewTicker(100 * time.Millisecond)
	} else {
		ticker = time.NewTicker(1000 * time.Millisecond)
	}
	defer ticker.Stop()
	haveErr := false

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
			if !isStdoutTerminal {
				fmt.Printf(".")
			}
		case r, ok := <-results:
			done = !ok
			if ok {
				linesMu.Lock()
				if r.err != nil {
					haveErr = true
					lines[r.index] = r.err.Error()
				} else {
					lines[r.index] = "done"
				}
				linesMu.Unlock()
			}
		}
		if isStdoutTerminal {
			linesMu.Lock()
			for i := range lines {
				fmt.Fprintf(&writer, "  %2d: ", c.nodes[i])
				if lines[i] != "" {
					fmt.Fprintf(&writer, "%s", lines[i])
				} else {
					fmt.Fprintf(&writer, "%s", spinner[spinnerIdx%len(spinner)])
				}
				fmt.Fprintf(&writer, "\n")
			}
			linesMu.Unlock()
			writer.Flush(os.Stdout)
			spinnerIdx++
		}
	}

	if !isStdoutTerminal {
		fmt.Printf("\n")
		linesMu.Lock()
		for i := range lines {
			fmt.Printf("  %2d: %s\n", c.nodes[i], lines[i])
		}
		linesMu.Unlock()
	}

	if haveErr {
		log.Fatal("failed")
	}
}

var parameterRe = regexp.MustCompile(`{[^}]*}`)
var pgurlRe = regexp.MustCompile(`{pgurl(:[-0-9]+)?}`)

func (c *syncedCluster) pgurls(nodes []int) map[int]string {
	ips := make([]string, len(nodes))
	c.parallel("", len(nodes), 0, func(i int) ([]byte, error) {
		var err error
		ips[i], err = c.getInternalIP(nodes[i])
		return nil, err
	})

	m := make(map[int]string, len(ips))
	for i, ip := range ips {
		m[nodes[i]] = c.impl.nodeURL(c, ip, c.impl.nodePort(c, nodes[i]))
	}
	return m
}

func (c *syncedCluster) ssh(args []string) error {
	if len(c.nodes) != 1 {
		return fmt.Errorf("invalid number of nodes for ssh: %d", c.nodes)
	}

	allArgs := []string{
		fmt.Sprintf("%s@%s", c.user(c.nodes[0]), c.host(c.nodes[0])),
		"-i", filepath.Join(osUser.HomeDir, ".ssh", "google_compute_engine"),
		"-o", "StrictHostKeyChecking=no",
	}

	// Perform template expansion on the arguments. Currently, we only expand
	// "{pgurl:x}" templates, though additional expansions could be added.
	var urls map[int]string
	for _, arg := range args {
		arg := parameterRe.ReplaceAllStringFunc(arg, func(s string) string {
			m := pgurlRe.FindStringSubmatch(s)
			if m == nil {
				return s
			}

			if m[1] == "" {
				m[1] = "all"
			} else {
				m[1] = m[1][1:]
			}

			if urls == nil {
				urls = c.pgurls(allNodes(len(c.vms)))
			}

			nodes, err := listNodes(m[1], len(c.vms))
			if err != nil {
				return err.Error()
			}

			var result []string
			for _, i := range nodes {
				if url, ok := urls[i]; ok {
					result = append(result, url)
				}
			}
			return strings.Join(result, " ")
		})

		allArgs = append(allArgs, strings.Split(arg, " ")...)
	}

	cmd := exec.Command(`ssh`, allArgs...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (c *syncedCluster) stopLoad() {
	if c.loadGen == 0 {
		log.Fatalf("no load generator node specified for cluster: %s", c.name)
	}

	display := fmt.Sprintf("%s: stopping load", c.name)
	c.parallel(display, 1, 0, func(i int) ([]byte, error) {
		session, err := newSSHSession(c.user(c.loadGen), c.host(c.loadGen))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		cmd := fmt.Sprintf("kill -9 $(lsof -t -i :%d -i :%d) 2>/dev/null || true",
			cockroach{}.nodePort(c, c.nodes[i]),
			cassandra{}.nodePort(c, c.nodes[i]))
		return session.CombinedOutput(cmd)
	})
}

func (c *syncedCluster) parallel(display string, count, concurrency int, fn func(i int) ([]byte, error)) {
	if concurrency == 0 || concurrency > count {
		concurrency = count
	}

	type result struct {
		index int
		out   []byte
		err   error
	}

	results := make(chan result, count)
	var wg sync.WaitGroup
	wg.Add(count)

	var index int
	startNext := func() {
		go func(i int) {
			defer wg.Done()
			out, err := fn(i)
			results <- result{i, out, err}
		}(index)
		index++
	}

	for index < concurrency {
		startNext()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var writer uiWriter
	out := io.Writer(os.Stdout)
	if display == "" {
		out = ioutil.Discard
	}

	var ticker *time.Ticker
	if isStdoutTerminal {
		ticker = time.NewTicker(100 * time.Millisecond)
	} else {
		ticker = time.NewTicker(1000 * time.Millisecond)
		fmt.Fprintf(out, "%s", display)
	}

	defer ticker.Stop()
	complete := make([]bool, count)
	var failed []result

	var spinner = []string{"|", "/", "-", "\\"}
	spinnerIdx := 0

	for done := false; !done; {
		select {
		case <-ticker.C:
			if !isStdoutTerminal {
				fmt.Fprintf(out, ".")
			}
		case r, ok := <-results:
			if r.err != nil {
				failed = append(failed, r)
			}
			done = !ok
			if ok {
				complete[r.index] = true
			}
			if index < count {
				startNext()
			}
		}

		if isStdoutTerminal {
			fmt.Fprint(&writer, display)
			var n int
			for i := range complete {
				if complete[i] {
					n++
				}
			}
			fmt.Fprintf(&writer, " %d/%d", n, len(complete))
			if !done {
				fmt.Fprintf(&writer, " %s", spinner[spinnerIdx%len(spinner)])
			}
			fmt.Fprintf(&writer, "\n")
			writer.Flush(out)
			spinnerIdx++
		}
	}

	if !isStdoutTerminal {
		fmt.Fprintf(out, "\n")
	}

	if len(failed) > 0 {
		sort.Slice(failed, func(i, j int) bool { return failed[i].index < failed[j].index })
		for _, f := range failed {
			fmt.Fprintf(os.Stderr, "%d: %s: %s\n", f.index, f.err, f.out)
		}
		log.Fatal("command failed")
	}
}
