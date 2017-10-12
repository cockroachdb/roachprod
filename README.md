# Setup

Make sure you have gcloud [installed](https://cloud.google.com/sdk/downloads) and configured (`gcloud auth list` to check, `gcloud auth login` to authenticate). You may want to update old installations (`gcloud components update`).

Building:
```
$ go get github.com/cockroachlabs/roachprod
```

# Synopsis

`roachprod` manages per-user clusters under the [cockroach-ephemeral](https://console.cloud.google.com/home/dashboard?project=cockroach-ephemeral) GCE project.

Simple workflow:

### Create a cluster:
```
$ roachprod create foo
Creating cluster marc-foo with 3 nodes
OK
marc-foo: 23h59m42s remaining
  marc-foo-0000
  marc-foo-0001
  marc-foo-0002
Syncing...
```

### Interact using crl-prod tools
```
$ crl-ssh marc-foo all df -h /
1: marc-foo-0000.us-east1-b.cockroach-ephemeral
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1        49G  1.2G   48G   3% /

2: marc-foo-0001.us-east1-b.cockroach-ephemeral
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1        49G  1.2G   48G   3% /

3: marc-foo-0002.us-east1-b.cockroach-ephemeral
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1        49G  1.2G   48G   3% /
```

### List clusters
```
$ roachprod list
marc-foo: 23h58m27s remaining
  marc-foo-0000
  marc-foo-0001
  marc-foo-0002
Syncing...
```

### Destroy cluster
```
$ roachprod destroy foo
Destroying cluster marc-foo with 3 nodes
OK
```

See `roachprod help <command>` for further details.

# Future improvements

* more configurable (machine sizes, zones)
* add warning/killing job (emails after ~1d, kills cluster after ~2d)
* make [roachperf](https://github.com/cockroachdb/roachperf) aware of `~/.roachprod/hosts` files
