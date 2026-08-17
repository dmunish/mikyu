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
	mu       sync.Mutex
	messages []Message
}

type Manager struct {
	mu     sync.RWMutex
	queues map[string]*Queue
}

var (
	ErrQueueExists   = errors.New("Queue with this name already exists.")
	ErrQueueNotFound = errors.New("Queue not found.")
	ErrQueueEmpty    = errors.New("Queue is empty.")
)

func NewManager() *Manager {
	return &Manager{queues: make(map[string]*Queue)}
}

func (m *Manager) NewQueue(name string) (*Queue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queues[name]; ok {
		return nil, ErrQueueExists
	}

	q := &Queue{name: name, messages: make([]Message, 0)}
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
	msg := Message{ID: id.String(), Payload: payload}

	q.mu.Lock()
	defer q.mu.Unlock()

	q.messages = append(q.messages, msg)
	return id.String(), nil
}

func (q *Queue) Peek() (Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return Message{}, ErrQueueEmpty
	}
	return q.messages[0], nil
}

func (q *Queue) Pop() (Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return Message{}, ErrQueueEmpty
	}

	msg := q.messages[0]
	q.messages[0] = Message{}
	q.messages = q.messages[1:]
	return msg, nil
}
