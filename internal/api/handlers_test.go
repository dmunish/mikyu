package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// request runs a call through the full mux (routing + handler).
func request(srv *Server, method, target, body string) *httptest.ResponseRecorder {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, target, nil)
	} else {
		r = httptest.NewRequest(method, target, strings.NewReader(body))
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, r)
	return rec
}

func TestCreateQueue(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		wantStatus int
	}{
		{"missing name", "/queues", http.StatusBadRequest},
		{"created", "/queues?name=orders", http.StatusCreated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := request(Mikyu(), http.MethodPost, tt.target, "")
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
		})
	}

	t.Run("duplicate returns 409", func(t *testing.T) {
		srv := Mikyu()
		request(srv, http.MethodPost, "/queues?name=orders", "") // create
		rec := request(srv, http.MethodPost, "/queues?name=orders", "")
		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
		}
	})
}

func TestPush(t *testing.T) {
	t.Run("missing payload returns 400", func(t *testing.T) {
		srv := Mikyu()
		request(srv, http.MethodPost, "/queues?name=orders", "") // create queue
		rec := request(srv, http.MethodPost, "/queues/orders/messages", "{}")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("unknown queue returns 404", func(t *testing.T) {
		rec := request(Mikyu(), http.MethodPost, "/queues/nope/messages", `{"payload":"x"}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("success returns 201 with id", func(t *testing.T) {
		srv := Mikyu()
		request(srv, http.MethodPost, "/queues?name=orders", "")
		rec := request(srv, http.MethodPost, "/queues/orders/messages", `{"payload":"hi"}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusCreated)
		}
		var resp map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("invalid JSON: %v", err)
		}
		if resp["id"] == "" {
			t.Fatal("expected id in response")
		}
	})
}

func TestPeekAndPop(t *testing.T) {
	t.Run("unknown queue returns 404", func(t *testing.T) {
		rec := request(Mikyu(), http.MethodPost, "/queues/nope/peek", "")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("peek status = %d, want %d", rec.Code, http.StatusNotFound)
		}
		rec = request(Mikyu(), http.MethodPost, "/queues/nope/pop", "")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("pop status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("empty queue returns 404", func(t *testing.T) {
		srv := Mikyu()
		request(srv, http.MethodPost, "/queues?name=empty", "")
		rec := request(srv, http.MethodPost, "/queues/empty/pop", "")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("pop status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("success returns 200", func(t *testing.T) {
		srv := Mikyu()
		request(srv, http.MethodPost, "/queues?name=orders", "")
		request(srv, http.MethodPost, "/queues/orders/messages", `{"payload":"hi"}`)

		rec := request(srv, http.MethodPost, "/queues/orders/peek", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("peek status = %d, want %d (fix handler to StatusOK)", rec.Code, http.StatusOK)
		}
		rec = request(srv, http.MethodPost, "/queues/orders/pop", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("pop status = %d, want %d (fix handler to StatusOK)", rec.Code, http.StatusOK)
		}
	})
}
