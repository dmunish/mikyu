package main

import (
	"log"

	"github.com/dmunish/mikyu/internal/server"
	"github.com/dmunish/mikyu/queue"
)

func main() {
	mgr, err := queue.Open("data")
	if err != nil {
		log.Fatal(err)
	}

	srv := server.New(mgr)
	log.Println("mikyud listening on :7420")
	if err := srv.Start(":7420"); err != nil {
		log.Fatal(err)
	}
}
