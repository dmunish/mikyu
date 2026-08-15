package queue

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gofrs/uuid/v5"
)

type Message struct {
	ID      string
	Payload string
}

type Queue struct {
	name     string
	messages []Message
}

type Manager struct {
	mu     sync.Mutex
	queues map[string]*Queue
}

func NewManager() *Manager {
	return &Manager{queues: make(map[string]*Queue)}
}

func (m *Manager) NewQueue(name string) (*Queue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queues[name]; ok {
		return nil, fmt.Errorf("Queue with name \"%s\" already exists.", name)
	}

	q := &Queue{name: name, messages: make([]Message, 0)}
	m.queues[name] = q
	return q, nil
}

func (m *Manager) GetQueue(name string) (*Queue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	q, ok := m.queues[name]
	if !ok {
		return nil, errors.New("Queue not found")
	}
	return q, nil
}

func (q *Queue) Push(payload string) (string, error) {
	id, _ := uuid.NewV7()

	msg := Message{ID: id.String(), Payload: payload}
	q.messages = append(q.messages, msg)
	return id.String(), nil
}

func (q *Queue) Peek() (Message, error) {
	if len(q.messages) == 0 {
		return Message{}, errors.New("Queue is empty")
	}
	return q.messages[0], nil
}

func (q *Queue) Pop() (Message, error) {
	if len(q.messages) == 0 {
		return Message{}, errors.New("Queue is empty")
	}

	msg := q.messages[0]
	q.messages[0] = Message{}
	q.messages = q.messages[1:]
	return msg, nil
}
