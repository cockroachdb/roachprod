package main

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/roachprod/roachprodng"
	"context"
	"time"
	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"os/user"
	"log"
	"github.com/pkg/errors"
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

type BinarySource interface {
	Download(ctx context.Context, binaries ...string) (map[string]string, error)
}

type RemoteSource binfetcher.Options

var _ BinarySource = RemoteSource{}

func (src RemoteSource) Download(ctx context.Context, binNames ...string) (map[string]string, error) {
	m := map[string]string{}
	for _, binName := range binNames {
		src.Binary = binName
		loc, err := binfetcher.Download(ctx, binfetcher.Options(src))
		if err != nil {
			return nil, errors.Wrapf(err, "while downloading %s", binName)
		}
		m[binName] = loc
	}
	return m, nil
}

// LocalDevelopmentSource checks $GOPATH/src/github.com/cockroachdb/cockroach
// for the requested binary and, if found, prefers that over downloading it.
// Note that this won't check that the binary was compiled for the right
// architecture.
type LocalDevelopmentSource RemoteSource

var _ BinarySource = LocalDevelopmentSource{}

func (src LocalDevelopmentSource) Download(ctx context.Context, binNames ...string) (map[string]string, error) {
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		user, err := user.Current()
		if err != nil {
			return nil, err
		}
		gopath = filepath.Join(user.HomeDir, "go")
	}

	localRepo := filepath.Join(gopath, "src/github.com/cockroachdb/cockroach")

	m := map[string]string{}
	for _, binName := range binNames {
		localBin := filepath.Join(localRepo, binName)
		if _, err := os.Stat(localBin); err == nil {
			log.Printf("using %s from %s", binName, localBin)
			m[binName] = localBin
			break
		} else if !os.IsNotExist(err) {
			return nil, errors.Wrapf(err, "stat %s", localBin)
		}
		log.Printf("%s not found in local repo; downloading", binName)
		loc, err := RemoteSource(src).Download(ctx, binName)
		if err != nil {
			return nil, err
		}
		m[binName] = loc[binName]
	}

	return m, nil
}

