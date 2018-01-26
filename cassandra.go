package main

import (
	"bufio"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"os"
	"time"
)

type cassandra struct{}

func (cassandra) start(c *syncedCluster) {
	yamlPath, err := makeCassandraYAML(c)
	if err != nil {
		log.Fatal(err)
	}
	c.put(yamlPath, "./cassandra.yaml")
	_ = os.Remove(yamlPath)

	display := fmt.Sprintf("%s: starting cassandra (be patient)", c.name)
	nodes := c.serverNodes()
	c.parallel(display, len(nodes), 1, func(i int) ([]byte, error) {
		host := c.host(nodes[i])
		user := c.user(nodes[i])

		if err := func() error {
			session, err := newSSHSession(user, host)
			if err != nil {
				return err
			}
			defer session.Close()

			cmd := c.env + ` cassandra` +
				` -Dcassandra.config=file://${PWD}/cassandra.yaml` +
				` -Dcassandra.ring_delay_ms=3000` +
				` > cassandra.stdout 2> cassandra.stderr`
			_, err = session.CombinedOutput(cmd)
			return err
		}(); err != nil {
			return nil, err
		}

		for {
			up, err := func() (bool, error) {
				session, err := newSSHSession(user, host)
				if err != nil {
					return false, err
				}
				defer session.Close()

				cmd := `nc -z $(hostname) 9042`
				if _, err := session.CombinedOutput(cmd); err != nil {
					return false, nil
				}
				return true, nil
			}()
			if err != nil {
				return nil, err
			}
			if up {
				break
			}
			time.Sleep(time.Second)
		}
		return nil, nil
	})
}

func (cassandra) nodeURL(_ *syncedCluster, host string, port int) string {
	return fmt.Sprintf("'cassandra://%s:%d'", host, port)
}

func (cassandra) nodePort(c *syncedCluster, index int) int {
	if c.isLocal() {
		// TODO(peter): This will require a bit of work to adjust ports in
		// cassandra.yaml.
	}
	return 9042
}

func makeCassandraYAML(c *syncedCluster) (string, error) {
	ip, err := c.getInternalIP(c.serverNodes()[0])
	if err != nil {
		return "", err
	}

	f, err := ioutil.TempFile("", "cassandra.yaml")
	if err != nil {
		return "", err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	w.WriteString(cassandraDefaultYAML)
	defer w.Flush()

	t, err := template.New("cassandra.yaml").Parse(cassandraDiffYAML)
	if err != nil {
		log.Fatal(err)
	}
	m := map[string]interface{}{
		"Seeds": ip,
	}
	if err := t.Execute(w, m); err != nil {
		log.Fatal(err)
	}
	return f.Name(), nil
}
