package main

import (
	"os"

	"github.com/wegman-software/osm2pgsql-go/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
