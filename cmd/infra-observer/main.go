// Command infra-observer is the single binary behind every process of the
// platform. Subcommands select the role; see `infra-observer help`.
package main

import (
	"fmt"
	"os"
)

// version is overridden at build time via -ldflags.
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return nil
	}
	rest := args[1:]
	switch args[0] {
	case "version":
		fmt.Println(version)
		return nil
	case "config":
		return cmdConfig(rest)
	case "inventory":
		return cmdInventory(rest)
	case "migrate":
		return cmdMigrate(rest)
	case "collector":
		return cmdCollector(rest)
	case "simulator":
		return cmdSimulator(rest)
	case "scenario":
		return cmdScenario(rest)
	case "script":
		return cmdScript(rest)
	case "api":
		return cmdAPI(rest)
	case "processor":
		return cmdProcessor(rest)
	case "automation-worker":
		return cmdAutomationWorker(rest)
	case "deadletter":
		return cmdDeadLetter(rest)
	case "replay":
		return cmdReplay(rest)
	case "help", "-h", "--help":
		usage()
		return nil
	}
	return fmt.Errorf("unknown command %q (try `infra-observer help`)", args[0])
}

func usage() {
	fmt.Println(`usage: infra-observer <command> [flags]

commands:
  config validate     load and validate the configuration and inventory
  inventory list      print registered devices
  migrate             apply pending database migrations
  collector           poll devices and publish raw observations
  simulator           run the simulated lab (SNMP agents + scenario control)
  api                 serve the REST API
  processor           run the processing pipeline (normalize, enrich, state, rules, persist)
  automation-worker   react to alerts: policies, proposals, integrations, adapters
  scenario            apply a scenario to the running simulator
  deadletter          list, replay or purge dead-lettered messages
  replay              republish a stream window so the pipeline processes it again
  script test|test-all|validate|list   develop and check JavaScript extensions
  version             print the build version

Every command accepts --config (default $INFRA_OBSERVER_CONFIG or configs/config.yaml).`)
}
