package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/pkg/errors"
	"math"

	"golang.org/x/sync/errgroup"
)

const (
	project = "cockroach-ephemeral"
	domain  = "@cockroachlabs.com"
	vmUser  = "cockroach"
)

// VMOpts is the set of options when creating VMs.
type VMOpts struct {
	UseLocalSSD    bool
	Lifetime       time.Duration
	MachineType    string
	GeoDistributed bool
}

func runJSONCommand(args []string, parsed interface{}) error {
	cmd := exec.Command("gcloud", args...)

	rawJSON, err := cmd.Output()
	if err != nil {
		return errors.Wrapf(err, "failed to run: gcloud %s", strings.Join(args, " "))
	}

	if err := json.Unmarshal(rawJSON, &parsed); err != nil {
		return errors.Wrapf(err, "failed to parse json %s", rawJSON)
	}

	return nil
}

type jsonVM struct {
	Name              string
	Labels            map[string]string
	CreationTimestamp time.Time
	NetworkInterfaces []struct {
		NetworkIP string
		AccessConfigs []struct {
			Name  string
			NatIP string
		}
	}
	Zone string
}

type JsonVMList []jsonVM

func (vms JsonVMList) Names() []string {
	ret := make([]string, len(vms))
	for i, vm := range vms {
		ret[i] = vm.Name
	}
	return ret
}

func (vms JsonVMList) Zones() []string {
	ret := make([]string, len(vms))
	for i, vm := range vms {
		ret[i] = vm.Zone
	}
	return ret
}

func listVMs() ([]jsonVM, error) {
	args := []string{"compute", "instances", "list", "--project", project, "--format", "json"}
	vms := make([]jsonVM, 0)

	if err := runJSONCommand(args, &vms); err != nil {
		return nil, err
	}
	return vms, nil
}

type jsonAuth struct {
	Account string
	Status  string
}

func findActiveAccount() (string, error) {
	args := []string{"auth", "list", "--format", "json", "--filter", "status~ACTIVE"}

	accounts := make([]jsonAuth, 0)
	if err := runJSONCommand(args, &accounts); err != nil {
		return "", err
	}

	if len(accounts) != 1 {
		return "", fmt.Errorf("no active accounts found, please configure gcloud")
	}

	if !strings.HasSuffix(accounts[0].Account, domain) {
		return "", fmt.Errorf("active account %q does no belong to domain %s", accounts[0].Account, domain)
	}

	username := strings.Split(accounts[0].Account, "@")[0]
	return username, nil
}

func createVMs(names []string, opts VMOpts) error {
	// Create GCE startup script file.
	filename, err := writeStartupScript()
	if err != nil {
		return errors.Wrapf(err, "could not write GCE startup script to temp file")
	}
	defer os.Remove(filename)

	if !opts.GeoDistributed {
		zones = []string{zones[0]}
	}

	totalNodes := float64(len(names))
	totalZones := float64(len(zones))
	nodesPerZone := int(math.Ceil(totalNodes / totalZones))

	ct := int(0)
	i := 0


	// Fixed args.
	args := []string{
		"compute", "instances", "create",
		"--subnet", "default",
		"--maintenance-policy", "MIGRATE",
		"--service-account", "21965078311-compute@developer.gserviceaccount.com",
		"--scopes", "default,storage-rw",
		"--image", "ubuntu-1604-xenial-v20171002",
		"--image-project", "ubuntu-os-cloud",
		"--boot-disk-size", "10",
		"--boot-disk-type", "pd-ssd",
	}

	// Dynamic args.
	if opts.UseLocalSSD {
		args = append(args, "--local-ssd", "interface=SCSI")
	}
	args = append(args, "--machine-type", opts.MachineType)
	args = append(args, "--labels", fmt.Sprintf("lifetime=%s", opts.Lifetime))

	args = append(args, "--metadata-from-file", fmt.Sprintf("startup-script=%s", filename))
	args = append(args, "--project", project)

	var g errgroup.Group

	// This is calculating the number of machines to allocate per zone by taking the ceiling of the the total number
	// of machines left divided by the number of zones left. If the the number of machines isn't
	// divisible by the number of zones, then the extra machines will be allocated one per zone until there are
	// no more extra machines left.
	for i < len(names) {
		argsWithZone := append(args[:len(args):len(args)], "--zone", zones[ct])
		ct++
		argsWithZone = append(argsWithZone, names[i:i+nodesPerZone]...)
		i += nodesPerZone

		totalNodes -= float64(nodesPerZone)
		totalZones -= 1
		nodesPerZone = int(math.Ceil(totalNodes / totalZones))

		g.Go(func() error {
			cmd := exec.Command("gcloud", argsWithZone...)

			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
			}
			return nil
		})

	}

	return g.Wait()
}

func deleteVMs(names []string, zones []string) error {
	zoneMap := make(map[string][]string)
	for i, name := range names {
		zoneMap[zones[i]] = append(zoneMap[zones[i]], name)
	}

	var g errgroup.Group

	for zone, names := range zoneMap {
		args := []string{
			"compute", "instances", "delete",
			"--delete-disks", "all",
		}

		args = append(args, "--project", project)
		args = append(args, "--zone", zone)
		args = append(args, names...)

		g.Go(func() error {
			cmd := exec.Command("gcloud", args...)

			output, err := cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
			}
			return nil
		})
	}

	return g.Wait()
}

func extendVM(name string, zone string, lifetime time.Duration) error {
	args := []string{"compute", "instances", "add-labels"}

	args = append(args, "--project", project)
	args = append(args, "--zone", zone)
	args = append(args, "--labels", fmt.Sprintf("lifetime=%s", lifetime))
	args = append(args, name)

	cmd := exec.Command("gcloud", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}
	return nil
}

func cleanSSH() error {
	args := []string{"compute", "config-ssh", "--project", project, "--quiet", "--remove"}
	cmd := exec.Command("gcloud", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}
	return nil
}

func configSSH() error {
	args := []string{"compute", "config-ssh", "--project", project, "--quiet"}
	cmd := exec.Command("gcloud", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}
	return nil
}
