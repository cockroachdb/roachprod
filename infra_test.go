package main

import (
	"os"

	"context"
	"testing"
	"time"

	"golang.org/x/sync/errgroup" // TODO: vendor

	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/cockroachdb/roachprod/monitor"
	"github.com/cockroachdb/roachprod/roachprodng"
	"github.com/pkg/errors"
)

// TestPartitioningRoachmartBareBones runs a three-locality geo-distributed
// cluster and on it, the `roachmart` workload.
//
// NB: this test serves as an example to show the low-level APIs. With enough
// similar tests, it's likely that a common abstraction for geo-distributed
// testing emerges.
func TestPartitioningRoachmartBareBones(t *testing.T) {
	// NB: It's not clear that context cancellation is a good way to organize
	// the duration of this (or any) test.
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	localities := []string{
		"us-east1", "us-east2", "us-west1",
	}

	var opts []roachprodng.Option
	for _, loc := range localities {
		opts = append(opts, roachprodng.GCE{Locality: loc})
	}

	c, err := roachprodng.Create(ctx, opts...)

	if err != nil {
		t.Fatal(err)
	}

	// NB: this can be made a lot more ergonomical (pass options and a list of
	// binaries, receive the file names plus a cleanup closure).
	binaries := map[string]string{}
	for _, binary := range []string{"cockroach", "workload"} {
		binOpts := binfetcher.Options{
			Binary:  binary,
			Version: "LATEST",
			GOOS:    "linux",
			GOARCH:  "amd64",
		}

		loc, err := binfetcher.Download(ctx, binOpts)
		if err != nil {
			t.Fatal(errors.Wrap(err, binary))
		}
		defer func() {
			_ = os.Remove(loc)
		}()
		binaries[binary] = loc
	}

	for dest, src := range binaries {
		if err := c.Put(ctx, c.All(), src, dest); err != nil {
			t.Fatal(errors.Wrap(err, src))
		}
	}

	if err := c.Run(
		ctx,
		c.All(),
		"./cockroach start --insecure --store {{Store 0}} --join {{Addr 0}}",
		roachprodng.Detach(),
	); err != nil {
		t.Fatal(err)
	}

	if err := c.Run(ctx, c.Node(0), "./cockroach init"); err != nil {
		t.Fatal(err)
	}

	pgUrls := c.PGUrls(c.All(), 26257)

	// Set up enterprise license and debug-friendly cluster settings.
	// The information might have to come from env variables or flags.
	if err := SetupTestingClusterSettings(pgUrls[0]); err != nil {
		t.Fatal(err)
	}

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return monitor.MakeSQLUptimeMonitor(monitor.Options{
			Targets:        pgUrls,
			CheckFrequency: 10 * time.Second,
		}).Run(ctx)
	})

	c.Run(
		ctx,
		c.Node(0),
		// NB: worth thinking about a way to handle creating (and caching) the
		// fixtures on demand. The test may run a lot longer in that case, and
		// the context-based cancellation would not work well (but we could just
		// touch the fixtures early). Having to
		// manually regenerate fixtures would suck.
		"./workload fixtures load roachmart --localities 0,1,2 --scale 10",
	)

	for i := range localities {
		i := i // copy for the goroutine
		g.Go(func() error {
			// NB: for long-running tests, better to run this detached and to
			// use a monitor pattern (i.e. launch a process that terminates when
			// it's done, and check in on it periodically).
			//
			// I'd prefer that as a default in general (you could run tests for
			// weeks without worrying about the SSH session dropping, which is
			// great for manual tests), but it's overkill for some tests.
			//
			// NB: decide how to print the output of these tests. Could try to
			// use subtests instead of the errgroup (but we may generally want
			// failfast all behavior).
			return c.Run(
				ctx,
				c.Node(i),
				"./workload run roachmart {{PGUrl 0 26257}} --locality ${{Locality}}",
				roachprodng.V("Locality", i))
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}

// Examples of some high-level tests that don't particularly care about setting
// up the cluster themselves.
//
// NB: a nightly TeamCity job retrieves the list of top-level tests matching
// `^(Test|Benchmark)Nightly` and spins up jobs for them, so adding to the code
// below is all the work required.

// TestNightlyWorkload spins up clusters for a variety of workloads and
// benchmarks them (producing output similar to a Go benchmark).
//
// NB: Go benchmark functions are a poor fit here as we want to run all the
// subtests in parallel.
func TestNightlyWorkload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Hour)
	defer cancel()

	for _, workload := range []struct {
		name, cmd string
	}{
		{"kv_0", "kv --read-percent=0 --splits=1000 --concurrency=384 --duration=2h"},
		{"kv_95", "kv --read-percent=95 --splits=1000 --concurrency=384 --duration=2h"},
	} {

		t.Run(workload.name, func(t *testing.T) {
			// NB: it's unclear from the docs how much parallelism the test
			// harness actually uses (but it's likely related to
			// runtime.GOMAXPROCS?).
			t.Parallel()

			// RunPerf sets up a benchmarking cluster, sets up the fixture and
			// runs the specified workload. It saves the output to the artifacts
			// directory and perhaps sends the results somewhere (though perhaps
			// this is a more appropriate task for the CI runner).
			// It probably makes sense for it to append a subtest that gives a
			// synopsis of the configuration (assuming we'll soon start running
			// the same thing in multiple setups).
			if err := RunNightlyPerf(ctx, workload.cmd, TestHelperOptions{
				Nodes:   6, // cluster size
				Runners: 3, // workload runners
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestNightlyStress sets up a cluster and stresses it via a barrage of
// diverse load generators and trouble makers.
func TestNightlyStress(t *testing.T) {
	const duration = 5 * time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Set up a single cluster that gets hit by multiple non-overlapping workloads.
	// Workloads are mapped to runners sequentially. If the list grows too large,
	// we could run through the list in (randomized) waves.
	c, err := SetupNightlyCluster(TestHelperOptions{
		Nodes:      6,
		Runners:    3,
		Expiration: duration + time.Hour, // auto-destruction deadline
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Destroy()

	for i, workload := range []struct {
		name, cmd string
	}{
		// Scrubber looks up the existing schemas and runs SCRUB on each of them,
		// until it is told to terminate.
		{"scrubber", "scrubber"},
		// The quintessential correctness test.
		{"bank", "bank"},
		// Some component that creates random schemas, runs random queries on
		// that schema, adds and drops random indexes, ...
		{"randomsyntax", "rsg"},
		// Periodically looks up a random table and changes its zone configs.
		{"randomrebalance", "rebalancer --frequency 15m"},
		// Pick a random primary key and then focus all writers on it for 10min.
		// Run three of these for more impact (they chose the primary key based
		// on cluster time, so they will be highly correlated).
		// NB: might be better as a targeted test.
		{"hotspot", "hotspot --size 1mb --switch-after 10m"},
		{"hotspot", "hotspot --size 1mb --switch-after 10m"},
		{"hotspot", "hotspot --size 1mb --switch-after 10m"},
	} {
		i := i // probably have to take a copy because of t.Parallel()?
		t.Run(workload.name, func(t *testing.T) {
			t.Parallel()

			// NB: should really create the fixtures first, i.e. add an init
			// phase to this test. Note also that some workloads don't need
			// a fixture, in which case that command should be a no-op (or
			// create some empty DB/table).
			c.Run(ctx, c.Node(0), "./workload fixtures load "+workload.cmd)

			// NB: fileHelper creates a file in the artifacts directory.
			f, err := fileHelper(t.Name())
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			if err := c.Run(
				ctx, c.Node(i), "./workload run "+workload.cmd, roachprodng.CombinedOutput(f),
			); err != nil {
				t.Fatal(err)
			}
		})
	}

	// Other ideas here: `dropflap`: imports some large table; drops it; repeat.
	// (Don't quite want to model that one through `workload` but it's just a
	// snippet of code taken from the verbose code at the top, using
	// `workload.Fixtures()`).
}

// TestThatIsAlsoACLICommand illustrates what it would look like to have a
// CLI-type workload that is called as a test. The workload gets a cluster of
// any size and has to figure out what it wants to do. As a concrete example,
// the impl of `roachperf test splits` would just assume that there's a running
// cockroach cluster formed by all of the nodes and run splits via SQL. A perf
// one could similarly launch a `workload kv run` on all of the nodes. Or
// options could be passed in. Or a cli command could raze the existing cluster
// and set up a new n-1 nodes cluster and one runner and then do whatever. The
// API doesn't limit this in any way (and in particular *Cluster is fully
// symmetric across all of its nodes -- it's pure hardware).
func TestThatIsAlsoACLICommand(t *testing.T) {
	c, err := SetupNightlyCluster(TestHelperOptions{
		Nodes: 5,
		Runners: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := SomeCLITest(context.TODO(), c); err != nil {
		t.Fatal(err)
	}
}

// A lot more things we may want to do. Run workloads and intentionally run
// background load, with a pass/fail criterion on how badly the foreground load
// is impacted. It's straightforward to write these given the building blocks
// here.
//
// Run chaos. Corrupt disks. Disturb the network. All of these fit nicely enough
// into the pattern but will need slightly different tooling, but at the end of
// the day it won't be very different from what you've seen so far.

