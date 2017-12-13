package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
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
	// This is the earliest creation and shortest lifetime across VMs.
	CreatedAt time.Time
	Lifetime  time.Duration
	VMs       VMList
}

func (c *Cluster) ExpiresAt() time.Time {
	return c.CreatedAt.Add(c.Lifetime)
}

func (c *Cluster) LifetimeRemaining() time.Duration {
	return time.Until(c.ExpiresAt())
}

func (c *Cluster) String() string {
	return fmt.Sprintf("%s: %d (%s)", c.Name, len(c.VMs), c.LifetimeRemaining().Round(time.Second))
}

func (c *Cluster) PrintDetails() {
	fmt.Printf("%s: ", c.Name)
	l := c.LifetimeRemaining().Round(time.Second)
	if l <= 0 {
		fmt.Printf("expired %s ago\n", -l)
	} else {
		fmt.Printf("%s remaining\n", l)
	}
	for _, vm := range c.VMs {
		fmt.Printf("  %s\t%s.%s.%s\t%s\t%s\n", vm.Name, vm.Name, vm.Zone, project, vm.PrivateIP, vm.PublicIP)
	}
}

type VM struct {
	Name      string
	CreatedAt time.Time
	Lifetime  time.Duration
	PrivateIP string
	PublicIP  string
	Zone       string
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
			cloud.Clusters[clusterName] = &Cluster{
				Name:      clusterName,
				User:      userName,
				CreatedAt: createdAt,
				Lifetime:  lifetime,
				VMs:       make([]VM, 0),
			}
		}

		c := cloud.Clusters[clusterName]
		c.VMs = append(c.VMs, VM{
			Name:      vm.Name,
			CreatedAt: createdAt,
			Lifetime:  lifetime,
			PrivateIP: privateIP,
			PublicIP:  publicIP,
			Zone:       zone,
		})
		if createdAt.Before(c.CreatedAt) {
			c.CreatedAt = createdAt
		}
		if lifetime < c.Lifetime {
			c.Lifetime = lifetime
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
	vmZones := make([]string, n, n)
	for i, vm := range c.VMs {
		vmNames[i] = vm.Name
		vmZones[i] = vm.Zone
	}

	return deleteVMs(vmNames, vmZones)
}

func extendCluster(c *Cluster, extension time.Duration) error {
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
