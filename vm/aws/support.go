package aws

import (
	"encoding/json"
	"log"
	"os/exec"
	"strings"

	"github.com/cockroachdb/roachprod/vm"
	"github.com/pkg/errors"
)

// Both M5 and I3 machines expose their EBS or local SSD volumes as NVMe block devices, but
// the actual device numbers vary a bit between the two types.
// This user-data script will create a filesystem, mount the data volume, and chmod 777.
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/nvme-ebs-volumes.html
const awsStartupScript = `#!/usr/bin/env bash
set -x
disknum=0
for d in $(ls /dev/nvme?n1); do
  if ! mount | grep ${d}; then
    let "disknum++"
    echo "Disk ${d} not mounted, creating..."
    mountpoint="/mnt/data${disknum}"
    mkdir -p "${mountpoint}"
    mkfs.ext4 ${d}
    mount -o discard,defaults ${d} ${mountpoint}
    chmod 777 ${mountpoint}
    echo "${d} ${mountpoint} ext4 discard,defaults 1 1" | tee -a /etc/fstab
  else
    echo "Disk ${disknum}: ${d} already mounted, skipping..."
  fi
done
if [ "${disknum}" -eq "0" ]; then
  echo "No disks mounted, creating /mnt/data1"
  mkdir -p /mnt/data1
  chmod 777 /mnt/data1
fi
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
	// force json output in case the user has overridden the default behavior
	args = append(args[:len(args):len(args)], "--output", "json")
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
