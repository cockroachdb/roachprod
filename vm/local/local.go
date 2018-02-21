package local

import (
	"fmt"
	"github.com/cockroachdb/roachprod/config"
	"github.com/cockroachdb/roachprod/install"
	"github.com/cockroachdb/roachprod/vm"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"
)

const ProviderName = config.Local

func init() {
	vm.Providers[ProviderName] = &Provider{}
}

// The local Provider just creates some stub VM objects
type Provider struct{}

// No-op implementation of ProviderFlags
type emptyFlags struct{}

// No-op
func (o *emptyFlags) CreateFlags(flags *pflag.FlagSet) {
}

// No-op
func (p *Provider) CleanSSH() error {
	return nil
}

// No-op
func (p *Provider) ConfigSSH() error {
	return nil
}

// Just create fake host-info entries in the local filesystem
func (p *Provider) Create(names []string, opts vm.CreateOpts) error {
	path := filepath.Join(os.ExpandEnv(config.DefaultHostDir), config.Local)
	file, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "problem creating file %s", path)
	}
	defer file.Close()

	// Align columns left and separate with at least two spaces.
	tw := tabwriter.NewWriter(file, 0, 8, 2, ' ', 0)
	tw.Write([]byte("# user@host\tlocality\n"))
	for i := 0; i < len(names); i++ {
		tw.Write([]byte(fmt.Sprintf(
			"%s@%s\t%s\n", config.OSUser.Username, "127.0.0.1", "region=local,zone=local")))
	}
	if err := tw.Flush(); err != nil {
		return errors.Wrapf(err, "problem writing file %s", path)
	}
	return nil
}

// No-op
func (p *Provider) Delete(vms vm.List) error {
	return nil
}

// Returns error
func (p *Provider) Extend(vms vm.List, lifetime time.Duration) error {
	return errors.New("local clusters have unlimited lifetime")
}

// No-op
func (p *Provider) FindActiveAccount() (string, error) {
	return "", nil
}

// Returns the no-op option values for this provider
func (p *Provider) Flags() vm.ProviderFlags {
	return &emptyFlags{}
}

// Construct N-many localhost VM instances, using SyncedCluster as a way to remember
// how many nodes we should have
func (p *Provider) List() (ret vm.List, _ error) {
	if sc, ok := install.Clusters[ProviderName]; ok {
		now := time.Now()
		for range sc.VMs {
			ret = append(ret, vm.VM{
				Name:      "localhost",
				CreatedAt: now,
				Lifetime:  time.Hour,
				PrivateIP: "127.0.0.1",
				PublicIP:  "127.0.0.1",
				Zone:      config.Local,
			})

		}
	}
	return
}

// The name of the Provider, which will also surface in VM.Provider
func (p *Provider) Name() string {
	return ProviderName
}
