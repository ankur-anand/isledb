// Command isledb runs an IsleDB role as a service. Run "isledb help" for
// the roles and "isledb <role> -h" for a role's flags.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime/debug"

	"github.com/ankur-anand/isledb/cmd/isledb/internal/cacherole"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/maintenancerole"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/readerrole"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/service"
	"github.com/ankur-anand/isledb/cmd/isledb/internal/writerrole"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

var roles = []service.Role{cacherole.Role, maintenancerole.Role, readerrole.Role, writerrole.Role}

// version is set by release builds; a go install build reports the module
// version it was built from.
var version = ""

func versionString() string {
	if version != "" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
		return info.Main.Version
	}
	return "unknown"
}

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "version" || os.Args[1] == "--version") {
		fmt.Println("isledb", versionString())
		return
	}
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
