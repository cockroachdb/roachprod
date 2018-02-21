package gce

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/cockroachdb/roachprod/config"
	"github.com/cockroachdb/roachprod/vm"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

const (
	project      = "cockroach-ephemeral"
	ProviderName = "gce"
)

func init() {
	vm.Providers[ProviderName] = &Provider{}
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

// Used to parse the gcloud responses
type jsonVM struct {
	Name              string
	Labels            map[string]string
	CreationTimestamp time.Time
	NetworkInterfaces []struct {
		NetworkIP     string
		AccessConfigs []struct {
			Name  string
			NatIP string
		}
	}
	Zone string
}

// Convert the JSON VM data into our common VM type
func (jsonVM *jsonVM) toVM() *vm.VM {
	var vmErrors []error
	var err error

	// Check "lifetime" label.
	var lifetime time.Duration
	if lifetimeStr, ok := jsonVM.Labels["lifetime"]; ok {
		if lifetime, err = time.ParseDuration(lifetimeStr); err != nil {
			vmErrors = append(vmErrors, vm.ErrNoExpiration)
		}
	} else {
		vmErrors = append(vmErrors, vm.ErrNoExpiration)
	}

	// Extract network information
	var publicIP, privateIP string
	if len(jsonVM.NetworkInterfaces) == 0 {
		vmErrors = append(vmErrors, vm.ErrBadNetwork)
	} else {
		privateIP = jsonVM.NetworkInterfaces[0].NetworkIP
		if len(jsonVM.NetworkInterfaces[0].AccessConfigs) == 0 {
			vmErrors = append(vmErrors, vm.ErrBadNetwork)
		} else {
			publicIP = jsonVM.NetworkInterfaces[0].AccessConfigs[0].NatIP
		}
	}

	// This is splitting and taking the last part of a url path,
	// which is the zone.
	zones := strings.Split(jsonVM.Zone, "/")
	zone := zones[len(zones)-1]

	return &vm.VM{
		Name:      jsonVM.Name,
		CreatedAt: jsonVM.CreationTimestamp,
		Errors:    vmErrors,
		DNS:       fmt.Sprintf("%s.%s.%s", jsonVM.Name, zone, project),
		Provider:  ProviderName,
		Lifetime:  lifetime,
		PrivateIP: privateIP,
		PublicIP:  publicIP,
		Zone:      zone,
	}
}

type jsonAuth struct {
	Account string
	Status  string
}

// User-configurable, provider-specific options
type providerOpts struct {
	MachineType string
	Zones       []string
}

func (o *providerOpts) CreateFlags(flags *pflag.FlagSet) {
	flags.StringVar(&o.MachineType, "machine-type", "n1-standard-4", "DEPRECATED")
	flags.MarkDeprecated("machine-type", "use "+ProviderName+"-machine-type instead")
	flags.StringSliceVar(&o.Zones, "zones", []string{"us-east1-b", "us-west1-b", "europe-west2-b"}, "DEPRECATED")
	flags.MarkDeprecated("zones", "use "+ProviderName+"-zones instead")

	flags.StringVar(&o.MachineType, ProviderName+"-machine-type", "n1-standard-4", "Machine type (see https://cloud.google.com/compute/docs/machine-types)")
	flags.StringSliceVar(&o.Zones, ProviderName+"-zones", []string{"us-east1-b", "us-west1-b", "europe-west2-b"}, "Zones for cluster")
}

type Provider struct {
	opts providerOpts
}

func (p *Provider) CleanSSH() error {
	args := []string{"compute", "config-ssh", "--project", project, "--quiet", "--remove"}
	cmd := exec.Command("gcloud", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}
	return nil
}

func (p *Provider) ConfigSSH() error {
	args := []string{"compute", "config-ssh", "--project", project, "--quiet"}
	cmd := exec.Command("gcloud", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}
	return nil
}

func (p *Provider) Create(names []string, opts vm.CreateOpts) error {
	// Create GCE startup script file.
	filename, err := writeStartupScript()
	if err != nil {
		return errors.Wrapf(err, "could not write GCE startup script to temp file")
	}
	defer os.Remove(filename)

	if !opts.GeoDistributed {
		p.opts.Zones = []string{p.opts.Zones[0]}
	}

	totalNodes := float64(len(names))
	totalZones := float64(len(p.opts.Zones))
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
	args = append(args, "--machine-type", p.opts.MachineType)
	args = append(args, "--labels", fmt.Sprintf("lifetime=%s", opts.Lifetime))

	args = append(args, "--metadata-from-file", fmt.Sprintf("startup-script=%s", filename))
	args = append(args, "--project", project)

	var g errgroup.Group

	// This is calculating the number of machines to allocate per zone by taking the ceiling of the the total number
	// of machines left divided by the number of zones left. If the the number of machines isn't
	// divisible by the number of zones, then the extra machines will be allocated one per zone until there are
	// no more extra machines left.
	for i < len(names) {
		argsWithZone := append(args[:len(args):len(args)], "--zone", p.opts.Zones[ct])
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

func (p *Provider) Delete(vms vm.List) error {
	zoneMap := make(map[string][]string)
	for _, v := range vms {
		if v.Provider != ProviderName {
			return errors.Errorf("%s received VM instance from %s", ProviderName, v.Provider)
		}
		zoneMap[v.Zone] = append(zoneMap[v.Zone], v.Name)
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

func (p *Provider) Extend(vms vm.List, lifetime time.Duration) error {
	// The gcloud command only takes a single instance.  Unlike Delete() above, we have to
	// perform the iteration here.
	for _, v := range vms {
		args := []string{"compute", "instances", "add-labels"}

		args = append(args, "--project", project)
		args = append(args, "--zone", v.Zone)
		args = append(args, "--labels", fmt.Sprintf("lifetime=%s", lifetime))
		args = append(args, v.Name)

		cmd := exec.Command("gcloud", args...)

		output, err := cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
		}
	}
	return nil
}

func (p *Provider) FindActiveAccount() (string, error) {
	args := []string{"auth", "list", "--format", "json", "--filter", "status~ACTIVE"}

	accounts := make([]jsonAuth, 0)
	if err := runJSONCommand(args, &accounts); err != nil {
		return "", err
	}

	if len(accounts) != 1 {
		return "", fmt.Errorf("no active accounts found, please configure gcloud")
	}

	if !strings.HasSuffix(accounts[0].Account, config.EmailDomain) {
		return "", fmt.Errorf("active account %q does no belong to domain %s",
			accounts[0].Account, config.EmailDomain)
	}

	username := strings.Split(accounts[0].Account, "@")[0]
	return username, nil
}

func (p *Provider) Flags() vm.ProviderFlags {
	return &p.opts
}

// Query gcloud to produce a list of VM info objects.
func (p *Provider) List() (vm.List, error) {
	args := []string{"compute", "instances", "list", "--project", project, "--format", "json"}

	// Run the command, extracting the JSON payload
	jsonVMS := make([]jsonVM, 0)
	if err := runJSONCommand(args, &jsonVMS); err != nil {
		return nil, err
	}

	// Now, convert the json payload into our common VM type
	vms := make(vm.List, len(jsonVMS))
	for i, jsonVM := range jsonVMS {
		vms[i] = *jsonVM.toVM()
	}

	return vms, nil
}

func (p *Provider) Name() string {
	return ProviderName
}
