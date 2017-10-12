package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"

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
	account, err := findActiveAccount()
	if err != nil {
		log.Printf("skipping hosts config due to bad username: %v", err)
		return nil
	}

	// Write all host files.
	for _, c := range cloud.Clusters {
		filename := path.Join(hd, c.Name)

		var buf bytes.Buffer
		for _, vm := range c.VMs {
			fmt.Fprintf(&buf, "%s@%s\n", account, vm.PublicIP)
		}

		if err := ioutil.WriteFile(filename, buf.Bytes(), 0755); err != nil {
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
