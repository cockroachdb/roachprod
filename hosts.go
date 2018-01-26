package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/pkg/errors"
)

const (
	defaultHostDir   = "${HOME}/.roachprod/hosts"
	local            = "local"
	sshKeyPathPrefix = "${HOME}/.ssh/roachprod"
)

func initHostDir() error {
	hd := os.ExpandEnv(defaultHostDir)
	return os.MkdirAll(hd, 0755)
}

func syncHosts(cloud *Cloud) error {
	hd := os.ExpandEnv(defaultHostDir)

	// We need the username used to log into instances.
	// gcloud uses the local username rather than the account username.
	user, err := user.Current()
	if err != nil {
		log.Printf("skipping hosts config due to bad username: %v", err)
		return nil
	}

	// Write all host files.
	for _, c := range cloud.Clusters {
		filename := path.Join(hd, c.Name)
		file, err := os.Create(filename)
		if err != nil {
			return errors.Wrapf(err, "problem creating file %s", filename)
		}

		// Align columns left and separate with at least two spaces.
		tw := tabwriter.NewWriter(file, 0, 8, 2, ' ', 0)
		tw.Write([]byte("# user@host\tlocality\n"))
		for _, vm := range c.VMs {
			tw.Write([]byte(fmt.Sprintf(
				"%s@%s\t%s\n", user.Username, vm.PublicIP, vm.locality())))
		}
		if err := tw.Flush(); err != nil {
			return errors.Wrapf(err, "problem writing file %s", filename)
		}
	}

	return gcHostsFiles(cloud)
}

func gcHostsFiles(cloud *Cloud) error {
	hd := os.ExpandEnv(defaultHostDir)
	files, err := ioutil.ReadDir(hd)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.Mode().IsRegular() {
			continue
		}
		if _, ok := cloud.Clusters[file.Name()]; ok {
			continue
		}

		filename := filepath.Join(hd, file.Name())
		if err = os.Remove(filename); err != nil {
			log.Printf("failed to remove file %s", filename)
		}
	}
	return nil
}

func newInvalidHostsLineErr(line string) error {
	return fmt.Errorf("invalid hosts line, expected <username>@<host> [locality], got %q", line)
}

func loadClusters() error {
	hd := os.ExpandEnv(defaultHostDir)
	files, err := ioutil.ReadDir(hd)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.Mode().IsRegular() {
			continue
		}

		filename := filepath.Join(hd, file.Name())
		contents, err := ioutil.ReadFile(filename)
		if err != nil {
			return errors.Wrapf(err, "could not read %s", filename)
		}
		lines := strings.Split(string(contents), "\n")

		c := &syncedCluster{
			name: file.Name(),
		}

		for _, l := range lines {
			fields := strings.Fields(l)
			if len(fields) == 0 {
				continue
			} else if len(fields[0]) > 0 && fields[0][0] == '#' {
				// Comment line.
				continue
			} else if len(fields) > 2 {
				return newInvalidHostsLineErr(l)
			}

			parts := strings.Split(fields[0], "@")
			var n, u string
			if len(parts) == 1 {
				user, err := user.Current()
				if err != nil {
					return errors.Wrapf(err, "failed to lookup current user")
				}
				u = user.Username
				n = parts[0]
			} else if len(parts) == 2 {
				u = parts[0]
				n = parts[1]
			} else {
				return newInvalidHostsLineErr(l)
			}

			var l string
			if len(fields) == 2 {
				l = fields[1]
			}

			c.vms = append(c.vms, n)
			c.users = append(c.users, u)
			c.localities = append(c.localities, l)
		}
		clusters[file.Name()] = c
	}

	clusters[local] = &syncedCluster{
		name: local,
	}
	return nil
}
