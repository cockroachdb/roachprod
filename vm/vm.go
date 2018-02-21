package vm

import (
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/cockroachdb/roachprod/config"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

// Abstracted representation of a specific machine instance.  This type is used across
// the various cloud providers supported by roachprod.
type VM struct {
	Name      string
	CreatedAt time.Time
	// If non-empty, indicates that some or all of the data in the VM instance
	// is not present or otherwise invalid.
	Errors   []error
	Lifetime time.Duration
	// The provider-internal DNS name for the VM instance
	DNS string
	// The name of the cloud provider that hosts the VM instance
	Provider  string
	PrivateIP string
	PublicIP  string
	Zone      string
}

// Error values for VM.Error
var (
	BadNetwork   = errors.New("could not determine network information")
	InvalidName  = errors.New("invalid VM name")
	NoExpiration = errors.New("could not determine expiration")
)

var regionRE = regexp.MustCompile(`(.*[^-])-?[a-z]$`)

func (vm *VM) IsLocal() bool {
	return vm.Zone == config.Local
}

func (vm *VM) Locality() string {
	var region string
	if vm.IsLocal() {
		region = vm.Zone
	} else if match := regionRE.FindStringSubmatch(vm.Zone); len(match) == 2 {
		region = match[1]
	} else {
		log.Fatalf("unable to parse region from zone %q", vm.Zone)
	}
	return fmt.Sprintf("region=%s,zone=%s", region, vm.Zone)
}

type List []VM

func (vl List) Len() int           { return len(vl) }
func (vl List) Swap(i, j int)      { vl[i], vl[j] = vl[j], vl[i] }
func (vl List) Less(i, j int) bool { return vl[i].Name < vl[j].Name }

// Extract all VM.Name entries from the List
func (vl List) Names() []string {
	ret := make([]string, len(vl))
	for i, vm := range vl {
		ret[i] = vm.Name
	}
	return ret
}

// Extract all VM.Zone entries from the List
func (vl List) Zones() []string {
	ret := make([]string, len(vl))
	for i, vm := range vl {
		ret[i] = vm.Zone
	}
	return ret
}

// CreateOpts is the set of options when creating VMs.
type CreateOpts struct {
	UseLocalSSD    bool
	Lifetime       time.Duration
	GeoDistributed bool
	VMProviders    []string
}

// A hook point for Providers to supply additional, provider-specific flags to various
// roachprod commands.  In general, the flags should be prefixed with the provider's name
// to prevent collision between similar options.
type ProviderFlags interface {
	// Configure a FlagSet with any options relevant to creating new vm instances.
	CreateFlags(*pflag.FlagSet)
}

type Provider interface {
	CleanSSH() error
	ConfigSSH() error
	Create(names []string, opts CreateOpts) error
	Delete(vms List) error
	Extend(vms List, lifetime time.Duration) error
	// Return the account name associated with the provider
	FindActiveAccount() (string, error)
	// Returns a hook point for extending top-level roachprod tooling flags
	Flags() ProviderFlags
	List() (List, error)
	// The name of the Provider, which will also surface in the top-level Providers map.
	Name() string
}

// All known Provider instances.  This is initialized by subpackage init() functions.
var Providers = map[string]Provider{}

// Sequentially execute an action against all known VM Providers.
func EachProvider(action func(Provider) error) error {
	for _, p := range Providers {
		err := action(p)
		if err != nil {
			return errors.Wrapf(err, "while executing: %s", p.Name())
		}
	}
	return nil
}

// Collate a collection of VMs by their provider and invoke the callback.
func FanOut(list List, action func(Provider, List) error) error {
	var m = map[string]List{}
	for _, vm := range list {
		m[vm.Provider] = append(m[vm.Provider], vm)
	}

	var g errgroup.Group
	for name, vms := range m {
		g.Go(func() error {
			p, ok := Providers[name]
			if !ok {
				return errors.Errorf("unknown provider name: %s", name)
			}
			return action(p, vms)
		})
	}

	return g.Wait()
}

// Verify that each active Provider is using the same account name and return it
var cachedActiveAccount string

// Query the active providers for the name of the user account.  We require that all account names
// between the providers agree, or this function will return an error.
func FindActiveAccount() (string, error) {
	// Memoize
	if len(cachedActiveAccount) > 0 {
		return cachedActiveAccount, nil
	}

	// Ask each Provider for its active account name
	providerAccounts := map[string]string{}
	err := EachProvider(func(p Provider) (err error) {
		providerAccounts[p.Name()], err = p.FindActiveAccount()
		return
	})
	if err != nil {
		return "", err
	}

	// Ensure that there is exactly one distinct, non-trivial value across all providers
	counts := map[string]int{}
	var lastAccount string
	for _, acct := range providerAccounts {
		if len(acct) > 0 {
			lastAccount = acct
			counts[acct]++
		}
	}

	switch len(counts) {
	case 0:
		return "", errors.New("no Providers returned any active accounts")
	case 1:
		cachedActiveAccount = lastAccount
		return lastAccount, nil
	default:
		// There's disagreement between the providers who the user account is
		return "", errors.Errorf("multiple active Provider accounts detected: %s", providerAccounts)
	}
}

// Resolve the Provider with the given name and execute the action.
func ForProvider(named string, action func(Provider) error) error {
	p, ok := Providers[named]
	if !ok {
		return errors.Errorf("unknown vm provider: %s", named)
	}
	if err := action(p); err != nil {
		return errors.Wrapf(err, "in provider: %s", named)
	}
	return nil
}

// Concurrently execute an action for each named Provider.
func ForProviders(named []string, action func(Provider) error) error {
	var g errgroup.Group
	for _, name := range named {
		g.Go(func() error {
			return ForProvider(name, action)
		})
	}
	return g.Wait()
}
