package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"text/tabwriter"

	"github.com/pkg/errors"
)

const (
	defaultHostDir   = "${HOME}/.roachprod/hosts"
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
