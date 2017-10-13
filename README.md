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
  marc-foo-0000   [marc-foo-0000.us-east1-b.cockroach-ephemeral]
  marc-foo-0001   [marc-foo-0001.us-east1-b.cockroach-ephemeral]
  marc-foo-0002   [marc-foo-0002.us-east1-b.cockroach-ephemeral]
Syncing...
```

### Interact using crl-prod tools
`roachprod` populates hosts files in `~/.roachprod/hosts`. These are used by `crl-prod` tools to map clusters to node addresses.

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

### Interact using `roachperf`
[roachperf](https://github.com/cockroachlabs/roachprod) consumes `~/.roachprod/hosts`.

```
# Add ssh-key
$ ssh-add ~/.ssh/google_compute_engine

$ roachperf marc-foo status
marc-foo: status 3/3
   1: not running
   2: not running
   3: not running
```

### SSH into hosts
`roachprod` uses `gcloud` to sync the list of hostnames to `~/.ssh/config` and setup keys.

```
$ ssh marc-foo-0000.us-east1-b.cockroach-ephemeral
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
