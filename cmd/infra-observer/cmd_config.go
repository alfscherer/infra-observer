package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/inventory"
)

// configFlag registers the shared --config flag.
func configFlag(fs *flag.FlagSet) *string {
	def := os.Getenv("INFRA_OBSERVER_CONFIG")
	if def == "" {
		def = "configs/config.yaml"
	}
	return fs.String("config", def, "path to the configuration file")
}

func cmdConfig(args []string) error {
	if len(args) == 0 || args[0] != "validate" {
		return fmt.Errorf("usage: infra-observer config validate [--config FILE]")
	}
	fs := flag.NewFlagSet("config validate", flag.ContinueOnError)
	path := configFlag(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	devices, err := inventory.LoadFile(cfg.Inventory.File)
	if err != nil {
		return err
	}
	fmt.Printf("ok: configuration %s valid, %d devices in %s\n", *path, len(devices), cfg.Inventory.File)
	return nil
}

func cmdInventory(args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return fmt.Errorf("usage: infra-observer inventory list [--config FILE]")
	}
	fs := flag.NewFlagSet("inventory list", flag.ContinueOnError)
	path := configFlag(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	devices, err := inventory.LoadFile(cfg.Inventory.File)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tTYPE\tSITE\tADDRESS\tPROFILE\tTAGS")
	for _, d := range devices {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", d.ID, d.DeviceType, d.Site, d.ManagementAddress, d.Collection.Profile, strings.Join(d.Tags, ","))
	}
	return tw.Flush()
}
