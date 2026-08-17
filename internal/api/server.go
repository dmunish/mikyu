package api

import (
	"net/http"

	"github.com/dmunish/mikyu/internal/queue"
)

type Server struct {
	mgr *queue.Manager
}

func Mikyu() *Server {
	return &Server{mgr: queue.NewManager()}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queues", s.createQueue)
	mux.HandleFunc("POST /queues/{name}/messages", s.push)
	mux.HandleFunc("POST /queues/{name}/pop", s.pop)
	return mux
}
