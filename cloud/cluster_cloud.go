package cloud

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/roachprod/config"
	"github.com/pkg/errors"
)

const vmNameFormat = "user-<clusterid>-<nodeid>"

type Cloud struct {
	Clusters map[string]*CloudCluster
	// Individual "bad" instances by category.
	InvalidName  JsonVMList
	NoExpiration JsonVMList
	BadNetwork   JsonVMList
}

func newCloud() *Cloud {
	return &Cloud{
		Clusters:     make(map[string]*CloudCluster),
		InvalidName:  make(JsonVMList, 0),
		NoExpiration: make(JsonVMList, 0),
		BadNetwork:   make(JsonVMList, 0),
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

type VM struct {
	Name      string
	CreatedAt time.Time
	Lifetime  time.Duration
	PrivateIP string
	PublicIP  string
	Zone      string
}

var regionRE = regexp.MustCompile(`(.*[^-])-?[a-z]$`)

func (vm *VM) Locality() string {
	var region string
	if vm.Zone == config.Local {
		region = config.Local
	} else if match := regionRE.FindStringSubmatch(vm.Zone); len(match) == 2 {
		region = match[1]
	} else {
		log.Fatalf("unable to parse region from zone %q", vm.Zone)
	}
	return fmt.Sprintf("region=%s,zone=%s", region, vm.Zone)
}

func (vm *VM) dns() string {
	if vm.Zone == config.Local {
		return vm.Name
	}
	return fmt.Sprintf("%s.%s.%s", vm.Name, vm.Zone, project)
}

type VMList []VM

func (vl VMList) Len() int           { return len(vl) }
func (vl VMList) Swap(i, j int)      { vl[i], vl[j] = vl[j], vl[i] }
func (vl VMList) Less(i, j int) bool { return vl[i].Name < vl[j].Name }

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
			cloud.InvalidName = append(cloud.InvalidName, vm)
			continue
		}

		// Check "lifetime" label.
		lifetimeStr, ok := vm.Labels["lifetime"]
		if !ok {
			cloud.NoExpiration = append(cloud.NoExpiration, vm)
			continue
		}

		lifetime, err := time.ParseDuration(lifetimeStr)
		if err != nil {
			cloud.NoExpiration = append(cloud.NoExpiration, vm)
			continue
		}
		createdAt := vm.CreationTimestamp

		// Check private/public IPs.
		if len(vm.NetworkInterfaces) == 0 || len(vm.NetworkInterfaces[0].AccessConfigs) == 0 {
			cloud.BadNetwork = append(cloud.BadNetwork, vm)
			continue
		}
		privateIP := vm.NetworkInterfaces[0].NetworkIP
		publicIP := vm.NetworkInterfaces[0].AccessConfigs[0].NatIP
		// This is splitting and taking the last part of a url path,
		// which is the zone.
		vmZones := strings.Split(vm.Zone, "/")
		zone := vmZones[len(vmZones)-1]
		if len(privateIP) == 0 || len(publicIP) == 0 {
			cloud.BadNetwork = append(cloud.BadNetwork, vm)
			continue
		}

		if _, ok := cloud.Clusters[clusterName]; !ok {
			cloud.Clusters[clusterName] = &CloudCluster{
				Name:      clusterName,
				User:      userName,
				CreatedAt: createdAt,
				Lifetime:  lifetime,
				VMs:       nil,
			}
		}

		c := cloud.Clusters[clusterName]
		c.VMs = append(c.VMs, VM{
			Name:      vm.Name,
			CreatedAt: createdAt,
			Lifetime:  lifetime,
			PrivateIP: privateIP,
			PublicIP:  publicIP,
			Zone:      zone,
		})
		if createdAt.Before(c.CreatedAt) {
			c.CreatedAt = createdAt
		}
		if lifetime < c.Lifetime {
			c.Lifetime = lifetime
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
			"%s@%s\t%s\n", config.OsUser.Username, "127.0.0.1", "region=local,zone=local")))
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
