package roachprodng

import (
	"context"
	"io"
)

type Cluster struct {
	config struct {
		gce []GCE
	}
}

type Option interface {
	apply(*Cluster)
}

type optionFn func(*Cluster)

func (o optionFn) apply(c *Cluster) {
	o(c)
}

func Create(ctx context.Context, opts ...Option) (*Cluster, error) {
	var c Cluster
	for _, opt := range opts {
		opt.apply(&c)
	}
	// TODO: actually create
	return &c, nil
}

type Nodes []int

func (c *Cluster) Node(ns ...int) Nodes {
	return Nodes(ns)
}

func (c *Cluster) All() Nodes {
	// TODO
	return nil
}

func (c *Cluster) Put(ctx context.Context, n Nodes, src, dest string) error {
	// TODO
	return nil
}

func (c *Cluster) Run(ctx context.Context, n Nodes, template string, opts ...RunOption) error {
	// TODO
	return nil
}

func (c *Cluster) Destroy() {
	// TODO
}

func (c *Cluster) PGUrls(n Nodes, port int) []string {
	// TODO
	return nil
}

type RunOption interface {
	apply(*runOptions)
}

type runOptions struct {
	detach         bool
	stdout, stderr io.WriteCloser
	v map[string]interface{}
}

type detachOpt struct{}

func Detach() detachOpt {
	return detachOpt{}
}

func (detachOpt) apply(opts *runOptions) {
	opts.detach = true
}

type combineOpt struct{
	io.WriteCloser
}

func CombinedOutput(w io.WriteCloser) combineOpt {
	return combineOpt{w}
}

func (opt combineOpt) apply(opts *runOptions) {
	opts.stderr = opt
	opts.stdout = opt
}

type vOpt struct{
	k string
	v interface{}
}


func V(k string, v interface{}) vOpt {
	return vOpt{k,v}
}

func (opt vOpt) apply(opts *runOptions) {
	if opts.v == nil {
		opts.v = map[string]interface{}{}
	}
	opts.v[opt.k] = opt.v

}

type GCE struct {
	Locality string
}

func (opt GCE) apply(c *Cluster) {
	c.config.gce = append(c.config.gce, opt)
}
