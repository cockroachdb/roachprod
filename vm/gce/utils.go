package gce

import (
	"io/ioutil"
)

// Startup script used to find/format/mount all local SSDs in GCE.
// Each disk is mounted to /mnt/data<disknum> and chmoded to all users.
const gceLocalSSDStartupScript = `#!/usr/bin/env bash
disknum=0
# Assume google.
for d in $(ls /dev/disk/by-id/google-local-ssd-*); do
  let "disknum++"
  grep -e "${d}" /etc/fstab > /dev/null
  if [ $? -ne 0 ]; then
    echo "Disk ${disknum}: ${d} not mounted, creating..."
    mountpoint="/mnt/data${disknum}"
    sudo mkdir -p "${mountpoint}"
    sudo mkfs.ext4 -F ${d}
    sudo mount -o discard,defaults ${d} ${mountpoint}
    echo "${d} ${mountpoint} ext4 discard,defaults 1 1" | sudo tee -a /etc/fstab
  else
    echo "Disk ${disknum}: ${d} already mounted, skipping..."
  fi
done
if [ "${disknum}" -eq "0" ]; then
  echo "No disks mounted, creating /mnt/data1"
  sudo mkdir -p /mnt/data1
fi

sudo chmod 777 /mnt/data1
# sshguard can prevent frequent ssh connections to the same host. Disable it.
sudo service sshguard stop
`

// write the startup script to a temp file.
// Returns the path to the file.
// After use, the caller should delete the temp file.
func writeStartupScript() (string, error) {
	tmpfile, err := ioutil.TempFile("", "gce-startup-script")
	if err != nil {
		return "", err
	}
	defer tmpfile.Close()

	if _, err := tmpfile.WriteString(gceLocalSSDStartupScript); err != nil {
		return "", err
	}
	return tmpfile.Name(), nil
}
