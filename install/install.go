package install

import (
	"bytes"
	"fmt"

	"github.com/hashicorp/go-version"
)

// Memoizes cluster info for install operations
var Clusters = map[string]*SyncedCluster{}

var installCmds = map[string]string{
	"cassandra": `
echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | \
	sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list;
curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -;
sudo apt-get update;
sudo apt-get install -y cassandra;
sudo service cassandra stop;
`,

	"gcc": `
sudo apt-get update;
sudo apt-get install -y gcc;
`,

	// graphviz and rlwrap are useful for pprof
	"go": `
sudo apt-get update
sudo apt-get install -y graphviz rlwrap

curl https://dl.google.com/go/go1.9.3.linux-amd64.tar.gz | sudo tar -C /usr/local -xz
echo 'export PATH=$PATH:/usr/local/go/bin' | sudo tee /etc/profile.d/go.sh > /dev/null
sudo chmod +x /etc/profile.d/go.sh
`,

	"haproxy": `
sudo apt-get update;
sudo apt-get install -y haproxy;
`,

	"ntp": `
sudo apt-get update;
sudo apt-get install -y \
  ntp \
  ntpdate;
`,

	"tools": `
sudo apt-get update;
sudo apt-get install -y \
  fio \
  iftop \
  iotop \
  sysstat \
  linux-tools-common \
  linux-tools-4.10.0-35-generic \
  linux-cloud-tools-4.10.0-35-generic;
`,
}

func Install(c *SyncedCluster, args []string) error {
	do := func(title, cmd string) error {
		var buf bytes.Buffer
		err := c.Run(&buf, &buf, c.Nodes, "installing "+title, cmd)
		if err != nil {
			fmt.Print(buf.String())
		}
		return err
	}

	for _, arg := range args {
		cmd, ok := installCmds[arg]
		if !ok {
			return fmt.Errorf("unknown tool %q", arg)
		}

		if err := do(arg, cmd); err != nil {
			return err
		}
	}
	return nil
}

func VersionSatifies(v *version.Version, constraintString string) bool {
	constraints, err := version.NewConstraint(constraintString)
	if err != nil {
		panic(err)
	}
	return constraints.Check(v)
}
