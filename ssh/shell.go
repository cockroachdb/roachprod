package ssh

import (
	"fmt"
	"regexp"
	"strings"
)

const shellMetachars = "|&;()<> \t\n$\\`"

func Escape(args []string) string {
	escaped := make([]string, len(args))
	for i := range args {
		if strings.ContainsAny(args[i], shellMetachars) {
			// Argument contains shell metacharacters. Double quote the
			// argument, and backslash-escape any characters that still have
			// meaning inside of double quotes.
			e := regexp.MustCompile("($`\\\\)").ReplaceAllString(args[i], `\$1`)
			escaped[i] = fmt.Sprintf(`"%s"`, e)
		} else {
			escaped[i] = args[i]
		}
	}
	return strings.Join(escaped, " ")
}
