package aws

import (
	"encoding/json"
	"log"
	"os/exec"
	"strings"

	"github.com/cockroachdb/roachprod/vm"
	"github.com/pkg/errors"
)

// We're using an M5 type machine, which exposes EBS volumes as though they were locally-attached NVMe
// block devices.  This user-data script will create a filesystem, mount the data volume, and chown it to
// the ubuntu user which will be running the cockroach binary.
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/nvme-ebs-volumes.html
const awsStartupScript = `#!/usr/bin/env bash
set -e
mkfs.ext4 /dev/nvme1n1
mkdir -p /mnt/data1
mount /dev/nvme1n1 /mnt/data1
chown -R ubuntu:ubuntu /mnt/data1
`

// runCommand is used to invoke an AWS command for which no output is expected.
func runCommand(args []string) error {
	cmd := exec.Command("aws", args...)

	_, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Println(string(exitErr.Stderr))
		}
		return errors.Wrapf(err, "failed to run: aws %s", strings.Join(args, " "))
	}
	return nil
}

// runJSONCommand invokes an aws command and parses the json output.
func runJSONCommand(args []string, parsed interface{}) error {
	cmd := exec.Command("aws", args...)

	rawJSON, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Println(string(exitErr.Stderr))
		}
		return errors.Wrapf(err, "failed to run: aws %s", strings.Join(args, " "))
	}

	if err := json.Unmarshal(rawJSON, &parsed); err != nil {
		return errors.Wrapf(err, "failed to parse json %s", rawJSON)
	}

	return nil
}

// splitMap splits a list of `key:value` pairs into a map.
func splitMap(data []string) (map[string]string, error) {
	ret := make(map[string]string, len(data))
	for _, part := range data {
		parts := strings.Split(part, ":")
		if len(parts) != 2 {
			return nil, errors.Errorf("Could not split Region:AMI: %s", part)
		}
		ret[parts[0]] = parts[1]
	}
	return ret, nil
}

// regionMap collates VM instances by their region.
func regionMap(vms vm.List) (map[string]vm.List, error) {
	// Fan out the work by region
	byRegion := make(map[string]vm.List)
	for _, m := range vms {
		region, err := zoneToRegion(m.Zone)
		if err != nil {
			return nil, err
		}
		byRegion[region] = append(byRegion[region], m)
	}
	return byRegion, nil
}

// zoneToRegion converts an availability zone like us-east-2a to the zone name us-east-2
func zoneToRegion(zone string) (string, error) {
	return zone[0 : len(zone)-1], nil
}
