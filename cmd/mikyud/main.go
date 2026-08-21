package main

import (
	"log"

	"github.com/dmunish/mikyu/internal/queue"
	"github.com/dmunish/mikyu/internal/server"
)

func main() {
	mgr := queue.NewManager()
	srv := server.New(mgr)

	log.Println("mikyud listening on :7420")
	if err := srv.Start(":7420"); err != nil {
		log.Fatal(err)
	}
}
