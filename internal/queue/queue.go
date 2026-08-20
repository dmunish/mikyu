package queue

import (
	"errors"
	"sync"

	"github.com/gofrs/uuid/v5"
)

type Message struct {
	ID      string
	Payload []byte
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
	ErrQueueExists   = errors.New("queue with this name already exists")
	ErrQueueNotFound = errors.New("queue not found")
	ErrQueueEmpty    = errors.New("queue is empty")
	ErrQueueClosed   = errors.New("queue is closed")
)

func NewManager() *Manager {
	return &Manager{queues: make(map[string]*Queue)}
}

func (m *Manager) NewQueue(name string, capacity int) (*Queue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queues[name]; ok {
		return nil, ErrQueueExists
	}

	q := &Queue{name: name, messages: make(chan Message, capacity)}
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

func (q *Queue) Push(payload []byte) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}

	q.messages <- Message{ID: id.String(), Payload: payload}
	return id.String(), nil
}

func (q *Queue) Pop() (Message, error) {
	select {
	case msg, ok := <-q.messages:
		if !ok {
			return Message{}, ErrQueueClosed
		}
		return msg, nil
	default:
		return Message{}, ErrQueueEmpty
	}
}
