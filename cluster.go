package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const vmNameFormat = "user-<clusterid>-<nodeid>"

type Cloud struct {
	Clusters map[string]*Cluster
	// Individual "bad" instances by category.
	InvalidName  JsonVMList
	NoExpiration JsonVMList
	BadNetwork   JsonVMList
}

func newCloud() *Cloud {
	return &Cloud{
		Clusters:     make(map[string]*Cluster),
		InvalidName:  make(JsonVMList, 0),
		NoExpiration: make(JsonVMList, 0),
		BadNetwork:   make(JsonVMList, 0),
	}
}

type Cluster struct {
	Name string
	User string
	// This is the earliest of all VM expirations.
	Expiration time.Time
	VMs        VMList
}

func (c *Cluster) Lifetime() time.Duration {
	return time.Until(c.Expiration)
}

func (c *Cluster) String() string {
	return fmt.Sprintf("%s: %d (%s)", c.Name, len(c.VMs), c.Lifetime().Round(time.Second))
}

func (c *Cluster) PrintDetails() {
	fmt.Printf("%s: ", c.Name)
	l := c.Lifetime().Round(time.Second)
	if l <= 0 {
		fmt.Printf("expired %s ago\n", -l)
	} else {
		fmt.Printf("%s remaining\n", l)
	}
	for _, vm := range c.VMs {
		fmt.Printf("  %s\t%s.%s.%s\t%s\t%s\n", vm.Name, vm.Name, zone, project, vm.PrivateIP, vm.PublicIP)
	}
}

type VM struct {
	Name       string
	Expiration time.Time
	PrivateIP  string
	PublicIP   string
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

func listCloud() (*Cloud, error) {
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
		lifetime, ok := vm.Labels["lifetime"]
		if !ok {
			cloud.NoExpiration = append(cloud.NoExpiration, vm)
			continue
		}

		dur, err := time.ParseDuration(lifetime)
		if err != nil {
			cloud.NoExpiration = append(cloud.NoExpiration, vm)
			continue
		}
		expiration := vm.CreationTimestamp.Add(dur)

		// Check private/public IPs.
		if len(vm.NetworkInterfaces) == 0 || len(vm.NetworkInterfaces[0].AccessConfigs) == 0 {
			cloud.BadNetwork = append(cloud.BadNetwork, vm)
			continue
		}
		privateIP := vm.NetworkInterfaces[0].NetworkIP
		publicIP := vm.NetworkInterfaces[0].AccessConfigs[0].NatIP

		if len(privateIP) == 0 || len(publicIP) == 0 {
			cloud.BadNetwork = append(cloud.BadNetwork, vm)
			continue
		}

		if _, ok := cloud.Clusters[clusterName]; !ok {
			cloud.Clusters[clusterName] = &Cluster{
				Name:       clusterName,
				User:       userName,
				Expiration: expiration,
				VMs:        make([]VM, 0),
			}
		}

		c := cloud.Clusters[clusterName]
		c.VMs = append(c.VMs, VM{
			Name:       vm.Name,
			Expiration: expiration,
			PrivateIP:  privateIP,
			PublicIP:   publicIP,
		})
		if expiration.Before(c.Expiration) {
			c.Expiration = expiration
		}
	}

	// Sort VMs for each cluster. We want to make sure we always have the same order.
	for _, c := range cloud.Clusters {
		sort.Sort(c.VMs)
	}
	return cloud, nil
}

func createCluster(name string, nodes int, opts VMOpts) error {
	vmNames := make([]string, nodes, nodes)
	for i := 0; i < nodes; i++ {
		// Start instance indexing at 1.
		vmNames[i] = fmt.Sprintf("%s-%0.4d", name, i+1)
	}

	return createVMs(vmNames, opts)
}

func destroyCluster(c *Cluster) error {
	n := len(c.VMs)
	vmNames := make([]string, n, n)
	for i, vm := range c.VMs {
		vmNames[i] = vm.Name
	}

	return deleteVMs(vmNames)
}
