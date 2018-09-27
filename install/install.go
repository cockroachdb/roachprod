package install

import (
	"bytes"
	"fmt"
	"sort"

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

	"confluent": `
sudo apt-get update;
sudo apt-get install -y default-jdk-headless;
curl https://packages.confluent.io/archive/5.0/confluent-oss-5.0.0-2.11.tar.gz | sudo tar -C /usr/local -xz;
sudo ln -s /usr/local/confluent-5.0.0 /usr/local/confluent;
`,

	"docker": `
sudo apt-get update;
sudo apt-get install  -y \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common;
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -;
sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable";

sudo apt-get update;
sudo apt-get install  -y docker-ce;
`,

	"gcc": `
sudo apt-get update;
sudo apt-get install -y gcc;
`,

	// graphviz and rlwrap are useful for pprof
	"go": `
sudo apt-get update;
sudo apt-get install -y graphviz rlwrap;

curl https://dl.google.com/go/go1.11.linux-amd64.tar.gz | sudo tar -C /usr/local -xz;
echo 'export PATH=$PATH:/usr/local/go/bin' | sudo tee /etc/profile.d/go.sh > /dev/null;
sudo chmod +x /etc/profile.d/go.sh;
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

	"zfs": `
sudo apt-get update;
sudo apt-get install -y \
  zfsutils-linux;
`,
}

func SortedCmds() []string {
	cmds := make([]string, 0, len(installCmds))
	for cmd := range installCmds {
		cmds = append(cmds, cmd)
	}
	sort.Strings(cmds)
	return cmds
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
