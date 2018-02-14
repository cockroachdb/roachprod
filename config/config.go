package config

import (
	"log"
	"os/user"
)

var (
	Binary      = "./cockroach"
	GcEmailOpts EmailOpts
	OsUser      *user.User
	Zones       []string
)

func init() {
	var err error
	OsUser, err = user.Current()
	if err != nil {
		log.Panic("Unable to determine OS user", err)
	}
}

// EmailOpts is the set of options needed to configure the email client.
type EmailOpts struct {
	From     string
	Host     string
	Port     int
	User     string
	Password string
}

// A sentinel value used to indicate that an installation should
// take place on the local machine.  Later in the refactoring,
// this ought to be replaced by a LocalCloudProvider or somesuch.
const (
	Local          = "local"
	DefaultHostDir = "${HOME}/.roachprod/hosts"
)
