package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/pkg/errors"
)

const (
	defaultHostDir = "${HOME}/.roachprod/hosts"
)

func hostDir() string {
	return os.ExpandEnv(defaultHostDir)
}

func initHostDir() error {
	return os.MkdirAll(hostDir(), 0755)
}

func syncHosts(cloud *Cloud) error {
	hd := hostDir()

	// Write all host files.
	for _, c := range cloud.Clusters {
		filename := path.Join(hd, c.Name)

		var buf bytes.Buffer
		for _, vm := range c.VMs {
			fmt.Fprintf(&buf, "%s.%s.%s\n", vm.Name, zone, project)
		}

		if err := ioutil.WriteFile(filename, buf.Bytes(), 0755); err != nil {
			return errors.Wrapf(err, "problem writing file %s", filename)
		}
	}

	// TODO(marc): GC host files without a corresponding cluster.
	return nil
}
