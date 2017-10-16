package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const (
	project = "cockroach-ephemeral"
	domain  = "@cockroachlabs.com"
	zone    = "us-east1-b"
	vmUser  = "cockroach"
)

// VMOpts is the set of options when creating VMs.
type VMOpts struct {
	UseLocalSSD bool
	Lifetime    time.Duration
	MachineType string
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
		NetworkIP     string
		AccessConfigs []struct {
			Name  string
			NatIP string
		}
	}
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

	// Fixed args.
	args := []string{
		"compute", "instances", "create",
		"--subnet", "default",
		"--maintenance-policy", "MIGRATE",
		"--service-account", "21965078311-compute@developer.gserviceaccount.com",
		"--scopes", "https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append",
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
	args = append(args, "--zone", zone)
	args = append(args, names...)

	cmd := exec.Command("gcloud", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
	}
	return nil
}

func deleteVMs(names []string) error {
	args := []string{
		"compute", "instances", "delete",
		"--delete-disks", "all",
	}

	args = append(args, "--project", project)
	args = append(args, "--zone", zone)
	args = append(args, names...)

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
