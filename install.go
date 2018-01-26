package main

import (
	"bytes"
	"fmt"
)

func install(c *syncedCluster, args []string) error {
	do := func(title, cmd string) error {
		var buf bytes.Buffer
		err := c.run(&buf, c.nodes, "installing "+title, cmd)
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
