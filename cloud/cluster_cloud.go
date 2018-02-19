package cloud

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/roachprod/config"
	"github.com/pkg/errors"
)

type Cloud struct {
	Clusters map[string]*CloudCluster
	// Any VM in this list can be expected to have at least one element
	// in its Errors field.
	BadInstances VMList
}

// Collate Cloud.BadInstances by errors.
func (c *Cloud) BadInstanceErrors() map[error]VMList {
	ret := map[error]VMList{}

	// Expand instances and errors
	for _, vm := range c.BadInstances {
		for _, err := range vm.Errors {
			if _, ok := ret[err]; !ok {
				ret[err] = make(VMList, 0)
			}
			ret[err] = append(ret[err], vm)
		}
	}

	// Sort each VMList to make the output prettier
	for _, v := range ret {
		sort.Sort(v)
	}

	return ret
}

func newCloud() *Cloud {
	return &Cloud{
		Clusters:     make(map[string]*CloudCluster),
		BadInstances: make(VMList, 0),
	}
}

// A CloudCluster is created by querying gcloud.
//
// TODO(benesch): unify with syncedCluster.
type CloudCluster struct {
	Name string
	User string
	// This is the earliest creation and shortest lifetime across VMs.
	CreatedAt time.Time
	Lifetime  time.Duration
	VMs       VMList
}

func (c *CloudCluster) ExpiresAt() time.Time {
	return c.CreatedAt.Add(c.Lifetime)
}

func (c *CloudCluster) LifetimeRemaining() time.Duration {
	return time.Until(c.ExpiresAt())
}

func (c *CloudCluster) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s: %d", c.Name, len(c.VMs))
	if !c.IsLocal() {
		fmt.Fprintf(&buf, " (%s)", c.LifetimeRemaining().Round(time.Second))
	}
	return buf.String()
}

func (c *CloudCluster) PrintDetails() {
	fmt.Printf("%s: ", c.Name)
	if !c.IsLocal() {
		l := c.LifetimeRemaining().Round(time.Second)
		if l <= 0 {
			fmt.Printf("expired %s ago\n", -l)
		} else {
			fmt.Printf("%s remaining\n", l)
		}
	} else {
		fmt.Printf("(no expiration)\n")
	}
	for _, vm := range c.VMs {
		fmt.Printf("  %s\t%s\t%s\t%s\n", vm.Name, vm.dns(), vm.PrivateIP, vm.PublicIP)
	}
}

func (c *CloudCluster) IsLocal() bool {
	return c.Name == config.Local
}

func namesFromVMName(name string) (string, string, error) {
	parts := strings.Split(name, "-")
	if len(parts) < 3 {
		return "", "", fmt.Errorf("expected VM name in the form %s, got %s", vmNameFormat, name)
	}
	return parts[0], strings.Join(parts[:len(parts)-1], "-"), nil
}

// Note that this **does not** initialize the local CloudCluster object unlike the
// original version.  That local-initialization behavior currently resides in main.go.
func ListCloud() (*Cloud, error) {
	vms, err := listVMs()
	if err != nil {
		return nil, err
	}

	cloud := newCloud()

	for _, vm := range vms {
		// Parse cluster/user from VM name.
		userName, clusterName, err := namesFromVMName(vm.Name)
		if err != nil {
			vm.Errors = append(vm.Errors, VMInvalidName)
		}

		// Anything with an error gets tossed into the BadInstances slice, and we'll correct
		// the problem later on.
		if len(vm.Errors) > 0 {
			cloud.BadInstances = append(cloud.BadInstances, vm)
			continue
		}

		if _, ok := cloud.Clusters[clusterName]; !ok {
			cloud.Clusters[clusterName] = &CloudCluster{
				Name:      clusterName,
				User:      userName,
				CreatedAt: vm.CreatedAt,
				Lifetime:  vm.Lifetime,
				VMs:       nil,
			}
		}

		// Bound the cluster creation time and overall lifetime to the earliest and/or shortest VM
		c := cloud.Clusters[clusterName]
		c.VMs = append(c.VMs, vm)
		if vm.CreatedAt.Before(c.CreatedAt) {
			c.CreatedAt = vm.CreatedAt
		}
		if vm.Lifetime < c.Lifetime {
			c.Lifetime = vm.Lifetime
		}
	}

	return cloud, nil
}

func createLocalCluster(name string, nodes int) error {
	path := filepath.Join(os.ExpandEnv(config.DefaultHostDir), name)
	file, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "problem creating file %s", path)
	}
	defer file.Close()

	// Align columns left and separate with at least two spaces.
	tw := tabwriter.NewWriter(file, 0, 8, 2, ' ', 0)
	tw.Write([]byte("# user@host\tlocality\n"))
	for i := 0; i < nodes; i++ {
		tw.Write([]byte(fmt.Sprintf(
			"%s@%s\t%s\n", config.OSUser.Username, "127.0.0.1", "region=local,zone=local")))
	}
	if err := tw.Flush(); err != nil {
		return errors.Wrapf(err, "problem writing file %s", path)
	}
	return nil
}

func CreateCluster(name string, nodes int, opts VMOpts) error {
	if name == config.Local {
		return createLocalCluster(name, nodes)
	}

	vmNames := make([]string, nodes, nodes)
	for i := 0; i < nodes; i++ {
		// Start instance indexing at 1.
		vmNames[i] = fmt.Sprintf("%s-%0.4d", name, i+1)
	}

	return createVMs(vmNames, opts)
}

func DestroyCluster(c *CloudCluster) error {
	if c.IsLocal() {
		// Local cluster destruction is handled in destroyCmd.
		return errors.New("local clusters cannot be destroyed")
	}

	n := len(c.VMs)
	vmNames := make([]string, n, n)
	vmZones := make([]string, n, n)
	for i, vm := range c.VMs {
		vmNames[i] = vm.Name
		vmZones[i] = vm.Zone
	}

	return deleteVMs(vmNames, vmZones)
}

func ExtendCluster(c *CloudCluster, extension time.Duration) error {
	if c.IsLocal() {
		return errors.New("local clusters have unlimited lifetime")
	}

	newLifetime := c.Lifetime + extension
	extendErrors := make([]error, len(c.VMs))
	var wg sync.WaitGroup

	wg.Add(len(c.VMs))

	for i := range c.VMs {
		go func(i int) {
			defer wg.Done()
			extendErrors[i] = extendVM(c.VMs[i].Name, c.VMs[i].Zone, newLifetime)
		}(i)
	}

	wg.Wait()

	// Combine all errors into one.
	var err error
	for _, e := range extendErrors {
		if err == nil {
			err = e
		} else if e != nil {
			err = errors.Wrapf(err, e.Error())
		}
	}
	return err
}
