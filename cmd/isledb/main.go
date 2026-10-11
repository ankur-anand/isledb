// Command isledb runs an IsleDB role as a service. Run "isledb help" for
// the roles and "isledb <role> -h" for a role's flags.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/ankur-anand/isledb/cmd/isledb/internal/cacherole"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

var roles = []service.Role{cacherole.Role}

func main() {
	err := service.Main(roles, os.Args[1:], os.Stdout)
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
	case errors.Is(err, service.ErrUsage):
		fmt.Fprintln(os.Stderr, "isledb:", err)
		fmt.Fprint(os.Stderr, service.Usage(roles))
		os.Exit(2)
	default:
		fmt.Fprintln(os.Stderr, "isledb:", err)
		os.Exit(1)
	}
}
