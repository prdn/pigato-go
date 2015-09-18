package main

import (
	pigato "../"
	"log"
	"os"
)

func main() {
	verbose := false
	if len(os.Args) > 1 && os.Args[1] == "-v" {
		verbose = true
	}

	pigato.BrokerStart(verbose)
	log.Println("W: interrupt received, shutting down...")
}
