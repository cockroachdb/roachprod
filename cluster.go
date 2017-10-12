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
	InvalidName  []jsonVM
	NoExpiration []jsonVM
}

func newCloud() *Cloud {
	return &Cloud{
		Clusters:     make(map[string]*Cluster),
		InvalidName:  make([]jsonVM, 0),
		NoExpiration: make([]jsonVM, 0),
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
		fmt.Printf("  %s\n", vm.Name)
	}
}

func namesFromVMName(name string) (string, string, error) {
	parts := strings.Split(name, "-")
	if len(parts) != 3 {
		return "", "", fmt.Errorf("expected VM name in the form %s, got %s", vmNameFormat, name)
	}
	return parts[0], parts[0] + "-" + parts[1], nil
}

func listCloud() (*Cloud, error) {
	vms, err := listVMs()
	if err != nil {
		return nil, err
	}

	cloud := newCloud()

	for _, vm := range vms {
		userName, clusterName, err := namesFromVMName(vm.Name)
		if err != nil {
			cloud.InvalidName = append(cloud.InvalidName, vm)
			continue
		}

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

		if _, ok := cloud.Clusters[clusterName]; !ok {
			cloud.Clusters[clusterName] = &Cluster{
				Name:       clusterName,
				User:       userName,
				Expiration: expiration,
				VMs:        make([]jsonVM, 0),
			}
		}

		c := cloud.Clusters[clusterName]
		c.VMs = append(c.VMs, vm)
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

func createCluster(name string, nodes int, lifetime time.Duration) error {
	vmNames := make([]string, nodes, nodes)
	for i := 0; i < nodes; i++ {
		vmNames[i] = fmt.Sprintf("%s-%0.4d", name, i)
	}

	return createVMs(vmNames, lifetime)
}

func destroyCluster(c *Cluster) error {
	n := len(c.VMs)
	vmNames := make([]string, n, n)
	for i, vm := range c.VMs {
		vmNames[i] = vm.Name
	}

	return deleteVMs(vmNames)
}
