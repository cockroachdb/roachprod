package install

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

	"github.com/cockroachdb/roachprod/config"
	"github.com/cockroachdb/roachprod/ssh"
	"github.com/cockroachdb/roachprod/ui"
	"github.com/pkg/errors"
)

type ClusterImpl interface {
	Start(c *SyncedCluster)
	NodeURL(c *SyncedCluster, host string, port int) string
	NodePort(c *SyncedCluster, index int) int
}

// A SyncedCluster is created from the information in the synced hosts file
// and is used as the target for installing and managing various software
// components.
//
// TODO(benesch): unify with CloudCluster.
type SyncedCluster struct {
	// name, vms, users, localities are populated at init time.
	Name       string
	VMs        []string
	Users      []string
	Localities []string
	// all other fields are populated in newCluster.
	Nodes   []int
	LoadGen int
	Secure  bool
	Env     string
	Args    []string
	Impl    ClusterImpl
}

func (c *SyncedCluster) host(index int) string {
	return c.VMs[index-1]
}

func (c *SyncedCluster) user(index int) string {
	return c.Users[index-1]
}

func (c *SyncedCluster) locality(index int) string {
	return c.Localities[index-1]
}

func (c *SyncedCluster) IsLocal() bool {
	return c.Name == config.Local
}

func (c *SyncedCluster) ServerNodes() []int {
	if c.LoadGen == -1 {
		return c.Nodes
	}
	newNodes := make([]int, 0, len(c.Nodes))
	for _, i := range c.Nodes {
		if i != c.LoadGen {
			newNodes = append(newNodes, i)
		}
	}
	return newNodes
}

// getInternalIP returns the internal IP address of the specified node.
func (c *SyncedCluster) GetInternalIP(index int) (string, error) {
	if c.IsLocal() {
		return c.host(index), nil
	}

	session, err := ssh.NewSSHSession(c.user(index), c.host(index))
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

func (c *SyncedCluster) Start() {
	c.Impl.Start(c)
}

func (c *SyncedCluster) Stop() {
	display := fmt.Sprintf("%s: stopping", c.Name)
	c.Parallel(display, len(c.Nodes), 0, func(i int) ([]byte, error) {
		session, err := ssh.NewSSHSession(c.user(c.Nodes[i]), c.host(c.Nodes[i]))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		cmd := `pkill -9 "cockroach|java|mongo|kv|ycsb" || true ;
`
		cmd += fmt.Sprintf("kill -9 $(lsof -t -i :%d -i :%d) 2>/dev/null || true ;\n",
			Cockroach{}.NodePort(c, c.Nodes[i]),
			Cassandra{}.NodePort(c, c.Nodes[i]))
		return session.CombinedOutput(cmd)
	})
}

func (c *SyncedCluster) Wipe() {
	display := fmt.Sprintf("%s: wiping", c.Name)
	c.Parallel(display, len(c.Nodes), 0, func(i int) ([]byte, error) {
		session, err := ssh.NewSSHSession(c.user(c.Nodes[i]), c.host(c.Nodes[i]))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		cmd := `pkill -9 "cockroach|java|mongo|kv|ycsb" || true ;
`
		cmd += fmt.Sprintf("kill -9 $(lsof -t -i :%d -i :%d) 2>/dev/null || true ;\n",
			Cockroach{}.NodePort(c, c.Nodes[i]),
			Cassandra{}.NodePort(c, c.Nodes[i]))
		if c.IsLocal() {
			cmd += fmt.Sprintf(`rm -fr ${HOME}/local/%d/data ;`, c.Nodes[i])
		} else {
			cmd += `find /mnt/data* -maxdepth 1 -type f -exec rm -f {} \; ;
rm -fr /mnt/data*/{auxiliary,local,tmp,cassandra,cockroach,cockroach-temp*,mongo-data} \; ;
`
		}
		return session.CombinedOutput(cmd)
	})
}

func (c *SyncedCluster) Status() {
	display := fmt.Sprintf("%s: status", c.Name)
	results := make([]string, len(c.Nodes))
	c.Parallel(display, len(c.Nodes), 0, func(i int) ([]byte, error) {
		session, err := ssh.NewSSHSession(c.user(c.Nodes[i]), c.host(c.Nodes[i]))
		if err != nil {
			results[i] = err.Error()
			return nil, nil
		}
		defer session.Close()

		cmd := fmt.Sprintf("out=$(lsof -i :%d -i :%d -sTCP:LISTEN",
			Cockroach{}.NodePort(c, c.Nodes[i]),
			Cassandra{}.NodePort(c, c.Nodes[i]))
		cmd += ` | awk '!/COMMAND/ {print $1, $2}' | sort | uniq);
vers=$(` + config.Binary + ` version 2>/dev/null | awk '/Build Tag:/ {print $NF}')
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
		fmt.Printf("  %2d: %s\n", c.Nodes[i], r)
	}
}

type nodeMonitorInfo struct {
	Index int
	Msg   string
}

func (c *SyncedCluster) Monitor() chan nodeMonitorInfo {
	ch := make(chan nodeMonitorInfo)
	nodes := c.ServerNodes()

	for i := range nodes {
		go func(i int) {
			session, err := ssh.NewSSHSession(c.user(nodes[i]), c.host(nodes[i]))
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
				Cockroach{}.NodePort(c, nodes[i]))

			// Give the session a valid stdin pipe so that nc won't exit immediately.
			inPipe, err := session.StdinPipe()
			if err != nil {
				ch <- nodeMonitorInfo{nodes[i], err.Error()}
				return
			}
			defer inPipe.Close()
			if err := session.Run(cmd); err != nil {
				ch <- nodeMonitorInfo{nodes[i], err.Error()}
				return
			}
		}(i)
	}

	return ch
}

func (c *SyncedCluster) Run(w io.Writer, nodes []int, title, cmd string) error {
	display := fmt.Sprintf("%s: %s", c.Name, title)
	errors := make([]error, len(nodes))
	results := make([]string, len(nodes))
	c.Parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		session, err := ssh.NewSSHSession(c.user(nodes[i]), c.host(nodes[i]))
		if err != nil {
			errors[i] = err
			results[i] = err.Error()
			return nil, nil
		}
		defer session.Close()

		nodeCmd := cmd
		if c.IsLocal() {
			nodeCmd = fmt.Sprintf("cd ${HOME}/local/%d ; %s", nodes[i], cmd)
		}

		out, err := session.CombinedOutput(nodeCmd)
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

func (c *SyncedCluster) Wait() error {
	display := fmt.Sprintf("%s: waiting for nodes to start", c.Name)
	errs := make([]error, len(c.Nodes))
	c.Parallel(display, len(c.Nodes), 0, func(i int) ([]byte, error) {
		for j := 0; j < 600; j++ {
			session, err := ssh.NewSSHSession(c.user(c.Nodes[i]), c.host(c.Nodes[i]))
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			defer session.Close()

			// Wait for the startup scripts to complete.
			out, err := session.CombinedOutput("systemctl show google-startup-scripts -p ActiveState")
			if err != nil {
				errs[i] = err
				return nil, nil
			}
			if strings.TrimSpace(string(out)) == "ActiveState=activating" {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			return nil, nil
		}
		errs[i] = errors.New("timed out after 5m")
		return nil, nil
	})

	var foundErr bool
	for i, err := range errs {
		if err != nil {
			fmt.Printf("  %2d: %v\n", c.Nodes[i], err)
			foundErr = true
		}
	}
	if foundErr {
		return errors.New("not all nodes booted successfully")
	}
	return nil
}

func (c *SyncedCluster) CockroachVersions() map[string]int {
	sha := make(map[string]int)
	var mu sync.Mutex

	display := fmt.Sprintf("%s: cockroach version", c.Name)
	nodes := c.ServerNodes()
	c.Parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		session, err := ssh.NewSSHSession(c.user(c.Nodes[i]), c.host(nodes[i]))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		cmd := config.Binary + " version | awk '/Build Tag:/ {print $NF}'"
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

func (c *SyncedCluster) RunLoad(cmd string, stdout, stderr io.Writer) error {
	if c.LoadGen == 0 {
		log.Fatalf("%s: no load generator node specified", c.Name)
	}

	display := fmt.Sprintf("%s: retrieving IP addresses", c.Name)
	nodes := c.ServerNodes()
	ips := make([]string, len(nodes))
	c.Parallel(display, len(nodes), 0, func(i int) ([]byte, error) {
		var err error
		ips[i], err = c.GetInternalIP(nodes[i])
		return nil, err
	})

	session, err := ssh.NewSSHSession(c.user(c.LoadGen), c.host(c.LoadGen))
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
		urls = append(urls, c.Impl.NodeURL(c, ip, c.Impl.NodePort(c, nodes[i])))
	}
	return session.Run("ulimit -n 16384; " + cmd + " " + strings.Join(urls, " "))
}

const progressDone = "=======================================>"
const progressTodo = "----------------------------------------"

func formatProgress(p float64) string {
	i := int(math.Ceil(float64(len(progressDone)) * (1 - p)))
	return fmt.Sprintf("[%s%s] %.0f%%", progressDone[i:], progressTodo[:i], 100*p)
}

func (c *SyncedCluster) Put(src, dest string) {
	// TODO(peter): Only put 10 nodes at a time. When a node completes, output a
	// line indicating that.
	fmt.Printf("%s: putting %s %s\n", c.Name, src, dest)

	type result struct {
		index int
		err   error
	}

	var writer ui.Writer
	results := make(chan result, len(c.Nodes))
	lines := make([]string, len(c.Nodes))
	var linesMu sync.Mutex

	var wg sync.WaitGroup
	for i := range c.Nodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			if c.IsLocal() {
				from, err := filepath.Abs(src)
				if err != nil {
					results <- result{i, err}
					return
				}
				to := fmt.Sprintf(os.ExpandEnv("${HOME}/local/%d/%s"), c.Nodes[i], dest)
				// Remove the destination if it exists, ignoring errors which we'll
				// handle via the os.Symlink() call.
				_ = os.Remove(to)
				results <- result{i, os.Symlink(from, to)}
				return
			}

			session, err := ssh.NewSSHSession(c.user(c.Nodes[i]), c.host(c.Nodes[i]))
			if err == nil {
				defer session.Close()
				err = ssh.SCPPut(src, dest, func(p float64) {
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
	if ui.IsStdoutTerminal {
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
			if !ui.IsStdoutTerminal {
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
		if ui.IsStdoutTerminal {
			linesMu.Lock()
			for i := range lines {
				fmt.Fprintf(&writer, "  %2d: ", c.Nodes[i])
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

	if !ui.IsStdoutTerminal {
		fmt.Printf("\n")
		linesMu.Lock()
		for i := range lines {
			fmt.Printf("  %2d: %s\n", c.Nodes[i], lines[i])
		}
		linesMu.Unlock()
	}

	if haveErr {
		log.Fatal("failed")
	}
}

func (c *SyncedCluster) Get(src, dest string) {
	// TODO(peter): Only get 10 nodes at a time. When a node completes, output a
	// line indicating that.
	fmt.Printf("%s: getting %s %s\n", c.Name, src, dest)

	type result struct {
		index int
		err   error
	}

	var writer ui.Writer
	results := make(chan result, len(c.Nodes))
	lines := make([]string, len(c.Nodes))
	var linesMu sync.Mutex

	var wg sync.WaitGroup
	for i := range c.Nodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			session, err := ssh.NewSSHSession(c.user(c.Nodes[i]), c.host(c.Nodes[i]))
			if err == nil {
				defer session.Close()
				dest := dest
				if len(c.Nodes) > 1 {
					base := fmt.Sprintf("%d.%s", c.Nodes[i], filepath.Base(dest))
					dest = filepath.Join(filepath.Dir(dest), base)
				}
				err = ssh.SCPGet(src, dest, func(p float64) {
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
	if ui.IsStdoutTerminal {
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
			if !ui.IsStdoutTerminal {
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
		if ui.IsStdoutTerminal {
			linesMu.Lock()
			for i := range lines {
				fmt.Fprintf(&writer, "  %2d: ", c.Nodes[i])
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

	if !ui.IsStdoutTerminal {
		fmt.Printf("\n")
		linesMu.Lock()
		for i := range lines {
			fmt.Printf("  %2d: %s\n", c.Nodes[i], lines[i])
		}
		linesMu.Unlock()
	}

	if haveErr {
		log.Fatal("failed")
	}
}

var parameterRe = regexp.MustCompile(`{[^}]*}`)
var pgurlRe = regexp.MustCompile(`{pgurl(:[-0-9]+)?}`)

func (c *SyncedCluster) pgurls(nodes []int) map[int]string {
	ips := make([]string, len(nodes))
	c.Parallel("", len(nodes), 0, func(i int) ([]byte, error) {
		var err error
		ips[i], err = c.GetInternalIP(nodes[i])
		return nil, err
	})

	m := make(map[int]string, len(ips))
	for i, ip := range ips {
		m[nodes[i]] = c.Impl.NodeURL(c, ip, c.Impl.NodePort(c, nodes[i]))
	}
	return m
}

func (c *SyncedCluster) Ssh(args []string) error {
	if len(c.Nodes) != 1 {
		return fmt.Errorf("invalid number of nodes for ssh: %d", len(c.Nodes))
	}

	allArgs := []string{
		"ssh",
		fmt.Sprintf("%s@%s", c.user(c.Nodes[0]), c.host(c.Nodes[0])),
		"-i", filepath.Join(config.OSUser.HomeDir, ".ssh", "google_compute_engine"),
		"-o", "StrictHostKeyChecking=no",
	}
	if c.IsLocal() {
		allArgs = append(allArgs, fmt.Sprintf("cd ${HOME}/local/%d ; ", c.Nodes[0]))
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
				urls = c.pgurls(allNodes(len(c.VMs)))
			}

			nodes, err := ListNodes(m[1], len(c.VMs))
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

	sshPath, err := exec.LookPath(allArgs[0])
	if err != nil {
		return err
	}
	return syscall.Exec(sshPath, allArgs, os.Environ())
}

func (c *SyncedCluster) stopLoad() {
	if c.LoadGen == 0 {
		log.Fatalf("no load generator node specified for cluster: %s", c.Name)
	}

	display := fmt.Sprintf("%s: stopping load", c.Name)
	c.Parallel(display, 1, 0, func(i int) ([]byte, error) {
		session, err := ssh.NewSSHSession(c.user(c.LoadGen), c.host(c.LoadGen))
		if err != nil {
			return nil, err
		}
		defer session.Close()

		cmd := fmt.Sprintf("kill -9 $(lsof -t -i :%d -i :%d) 2>/dev/null || true",
			Cockroach{}.NodePort(c, c.Nodes[i]),
			Cassandra{}.NodePort(c, c.Nodes[i]))
		return session.CombinedOutput(cmd)
	})
}

func (c *SyncedCluster) Parallel(display string, count, concurrency int, fn func(i int) ([]byte, error)) {
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

	var writer ui.Writer
	out := io.Writer(os.Stdout)
	if display == "" {
		out = ioutil.Discard
	}

	var ticker *time.Ticker
	if ui.IsStdoutTerminal {
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
			if !ui.IsStdoutTerminal {
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

		if ui.IsStdoutTerminal {
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

	if !ui.IsStdoutTerminal {
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
