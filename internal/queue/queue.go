package queue

import (
	"errors"
	"sync"

	"github.com/gofrs/uuid/v5"
)

type Message struct {
	ID      string
	Payload string
}

type Queue struct {
	name     string
	messages chan Message
}

type Manager struct {
	mu     sync.RWMutex
	queues map[string]*Queue
}

var (
	ErrQueueExists   = errors.New("Queue with this name already exists.")
	ErrQueueNotFound = errors.New("Queue not found.")
	ErrQueueEmpty    = errors.New("Queue is empty.")
	ErrQueueClosed   = errors.New("Queue is closed.")
)

func NewManager() *Manager {
	return &Manager{queues: make(map[string]*Queue)}
}

func (m *Manager) NewQueue(name string, cap int) (*Queue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queues[name]; ok {
		return nil, ErrQueueExists
	}

	q := &Queue{name: name, messages: make(chan Message, cap)}
	m.queues[name] = q
	return q, nil
}

func (m *Manager) GetQueue(name string) (*Queue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	q, ok := m.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return q, nil
}

func (q *Queue) Push(payload string) (string, error) {
	id, _ := uuid.NewV7()

	q.messages <- Message{ID: id.String(), Payload: payload}
	return id.String(), nil
}

func (q *Queue) Pop() (Message, error) {
	if len(q.messages) == 0 {
		return Message{}, ErrQueueEmpty
	}
	msg, ok := <-q.messages
	if !ok {
		return Message{}, ErrQueueClosed
	}
	return msg, nil
}
