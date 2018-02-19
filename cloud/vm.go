package cloud

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/cockroachdb/roachprod/config"
)

// Abstracted representation of a specific machine instance.  This type is used across
// the various cloud providers supported by roachprod.
type VM struct {
	Name      string
	CreatedAt time.Time
	// If non-empty, indicates that some or all of the data in the VM instance
	// is not present or otherwise invalid.
	Errors    []error
	Lifetime  time.Duration
	PrivateIP string
	PublicIP  string
	Zone      string
}

// Error values for VM.Error
var (
	VMBadNetwork   = errors.New("could not determine network information")
	VMInvalidName  = errors.New("invalid VM name")
	VMNoExpiration = errors.New("could not determine expiration")
)

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

// Extract all VM.Name entries from the VMList
func (vl VMList) Names() []string {
	ret := make([]string, len(vl))
	for i, vm := range vl {
		ret[i] = vm.Name
	}
	return ret
}

// Extract all VM.Zone entries from the VMList
func (vl VMList) Zones() []string {
	ret := make([]string, len(vl))
	for i, vm := range vl {
		ret[i] = vm.Zone
	}
	return ret
}

// VMOpts is the set of options when creating VMs.
type VMOpts struct {
	UseLocalSSD    bool
	Lifetime       time.Duration
	MachineType    string
	GeoDistributed bool
}
