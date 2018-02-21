package install

import (
	"regexp"
	"strings"
)

var parameterRe = regexp.MustCompile(`{[^}]*}`)
var pgurlRe = regexp.MustCompile(`{pgurl(:[-0-9]+)?}`)
var storeDirRe = regexp.MustCompile(`{store-dir}`)

type expander struct {
	urls map[int]string
}

func (e *expander) maybeExpandPgurl(c *SyncedCluster, s string) (string, bool) {
	m := pgurlRe.FindStringSubmatch(s)
	if m == nil {
		return s, false
	}

	if m[1] == "" {
		m[1] = "all"
	} else {
		m[1] = m[1][1:]
	}

	if e.urls == nil {
		e.urls = c.pgurls(allNodes(len(c.VMs)))
	}

	nodes, err := ListNodes(m[1], len(c.VMs))
	if err != nil {
		return err.Error(), true
	}

	var result []string
	for _, i := range nodes {
		if url, ok := e.urls[i]; ok {
			result = append(result, url)
		}
	}
	return strings.Join(result, " "), true
}

func (e *expander) maybeExpandStoreDir(c *SyncedCluster, s string) (string, bool) {
	return c.Impl.NodeDir(c, c.Nodes[0]), true
}

func (e *expander) expand(c *SyncedCluster, arg string) string {
	return parameterRe.ReplaceAllStringFunc(arg, func(s string) string {
		s, expanded := e.maybeExpandPgurl(c, s)
		if expanded {
			return s
		}
		s, _ = e.maybeExpandStoreDir(c, s)
		return s
	})
}
