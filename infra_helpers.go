package main

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/roachprod/roachprodng"
	"context"
	"time"
)

func SetupTestingClusterSettings(db string) error {
	// TODO
	return nil
}

func RunNightlyPerf(ctx context.Context, cmd string, opts TestHelperOptions) error {
	// TODO
	return nil
}

func SetupNightlyCluster(opts TestHelperOptions) (*roachprodng.Cluster, error) {
	// TODO
	return &roachprodng.Cluster{}, nil
}

type TestHelperOptions struct {
	Nodes, Runners int
	Expiration     time.Duration
}

func fileHelper(name string) (*os.File, error) {
	const artifactsDir = "./artifacts"
	return os.Create(filepath.Join(artifactsDir, name))
}

func SomeCLITest(ctx context.Context, c *roachprodng.Cluster) error {
	// Does something that users can run against their roachprod clusters.
	return nil
}