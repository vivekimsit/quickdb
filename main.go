package main

import (
	"flag"
	"log"

	"github.com/vivekimsit/quickdb/config"
	"github.com/vivekimsit/quickdb/server"
)

func setupFlags() {
	flag.StringVar(&config.Host, "host", "0.0.0.0", "host for the dice server")
	flag.IntVar(&config.Port, "port", 7379, "port for the dice server")
	flag.Parse()
}

func main() {
	setupFlags()
	log.Println("ready to run 🐇")
	server.RunSyncTCPServer()
}
