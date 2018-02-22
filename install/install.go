package install

import (
	"bytes"
	"fmt"

	"github.com/hashicorp/go-version"
)

// Memoizes cluster info for install operations
var Clusters = map[string]*SyncedCluster{}

func Install(c *SyncedCluster, args []string) error {
	do := func(title, cmd string) error {
		var buf bytes.Buffer
		err := c.Run(&buf, c.Nodes, "installing "+title, cmd)
		if err != nil {
			fmt.Print(buf.String())
		}
		return err
	}

	for _, arg := range args {
		switch arg {
		case "cassandra":
			cmd := `
echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | \
  sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list;
curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -;
sudo apt-get update;
sudo apt-get install -y cassandra;
sudo service cassandra stop;
`
			if err := do("cassandra", cmd); err != nil {
				return err
			}

		case "go":
			// graphviz and rlwrap are useful for pprof
			cmd := `
sudo apt-get update
sudo apt-get install -y graphviz rlwrap

curl https://dl.google.com/go/go1.9.3.linux-amd64.tar.gz | sudo tar -C /usr/local -xz
echo 'export PATH=$PATH:/usr/local/go/bin' | sudo tee /etc/profile.d/go.sh > /dev/null
sudo chmod +x /etc/profile.d/go.sh
`

			if err := do("go", cmd); err != nil {
				return err
			}

		case "mongodb":
			return fmt.Errorf("TODO(peter): unimplemented: mongodb")

		case "postgres":
			return fmt.Errorf("TODO(peter): unimplemented: postgres")

		case "tools":
			cmd := `
sudo apt-get update;
sudo apt-get install -y \
  fio \
  iftop \
  iotop \
  sysstat \
  linux-tools-common \
  linux-tools-4.10.0-35-generic \
  linux-cloud-tools-4.10.0-35-generic;
`
			if err := do("tools", cmd); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unknown tool %q", arg)
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
