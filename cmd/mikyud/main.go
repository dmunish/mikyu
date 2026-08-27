package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/dmunish/mikyu/internal/server"
	"github.com/dmunish/mikyu/queue"
)

func main() {
	mgr, err := queue.Open("data")
	if err != nil {
		log.Fatal(err)
	}
	defer mgr.Close()

	srv := server.New(mgr)

	ln, err := net.Listen("tcp", ":7420")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("received signal %v, shutting down...", sig)
		ln.Close()
		mgr.Close()
	}()

	log.Println("mikyud listening on :7420")
	if err := srv.Serve(ln); err != nil {
		log.Printf("server stopped: %v", err)
	}
}
