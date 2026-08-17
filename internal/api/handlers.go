package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/dmunish/mikyu/internal/queue"
)

func (s *Server) createQueue(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if _, err := s.mgr.NewQueue(name, 100000); err != nil {
		if errors.Is(err, queue.ErrQueueExists) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) push(w http.ResponseWriter, r *http.Request) {
	var body struct{ Payload string }
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Payload == "" {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}

	q, err := s.mgr.GetQueue(r.PathValue("name"))
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	id, err := q.Push(body.Payload)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

func (s *Server) pop(w http.ResponseWriter, r *http.Request) {
	q, err := s.mgr.GetQueue(r.PathValue("name"))
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	msg, err := q.Pop()
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, msg)
}
