package repl

import (
	"errors"
	"fmt"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/ui/command"
)

func shortID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

// resolveQueue resolves the queue argument for a read-only task verb.
func (s *replSession) resolveQueue(args []string, minAfter int) (string, []string, error) {
	if s.activeQueue != "" && len(args) <= minAfter {
		return s.activeQueue, args, nil
	}
	if len(args) == 0 {
		return "", nil, fmt.Errorf("queue required (or use 'use <queue>')")
	}
	return args[0], args[1:], nil
}

// taskRef resolves the task argument: an explicit record/ID, or the last
// task this session dequeued.
func (s *replSession) taskRef(args []string) (mikyu.TaskRef, string, []string, bool) {
	if len(args) == 0 && s.lastTaskID != "" {
		return mikyu.TaskID(s.lastTaskID), s.lastTaskID, args, true
	}
	if len(args) == 0 {
		return nil, "", args, false
	}
	ref, id := command.ParseTaskRef(args[0])
	return ref, id, args[1:], true
}

// queueFor resolves the queue argument for the mutating task verbs,
// falling back to the active queue and then the last task's queue.
func (s *replSession) queueFor(args []string) string {
	if len(args) == 2 {
		return args[0]
	}
	if s.activeQueue != "" {
		return s.activeQueue
	}
	return s.lastTaskQueue
}

func (s *replSession) handleEnqueue(args []string) {
	var opts command.EnqueueOptions
	fs := command.EnqueueFlagSet("enqueue", &opts)
	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		s.usage("enqueue [flags] [queue] <payload|@file|-> (" + err.Error() + ")")
		return
	}
	queue, rest, err := s.resolveQueue(rest, 1)
	if err != nil || len(rest) < 1 {
		s.usage("enqueue [flags] [queue] <payload|@file|->")
		return
	}
	task, n, err := command.Enqueue(s.ctx, s.client, queue, rest[0], opts)
	if err != nil {
		s.fail(err)
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf(
		"● Enqueued task %s to %q (%d bytes)", shortID(task.ID), queue, n,
	)))
}

func (s *replSession) handleDequeue(args []string) {
	var timeout time.Duration
	fs := command.NewFlagSet("dequeue")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "how long to wait for a due task")
	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		s.usage("dequeue <queue> [--timeout 30s] (" + err.Error() + ")")
		return
	}
	queue, _, err := s.resolveQueue(rest, 0)
	if err != nil {
		s.usage("dequeue <queue> [--timeout 30s]")
		return
	}
	if kind, err := s.kindOf(queue); err == nil && kind != mikyu.QueueKindTask {
		fmt.Println(errStyle.Render(fmt.Sprintf("dequeue requires a task queue; %q is a %s queue.", queue, kind)))
		return
	}
	task, err := command.Dequeue(s.ctx, s.client, queue, timeout)
	if err != nil {
		if errors.Is(err, mikyu.ErrTaskNotAvailable) {
			fmt.Println(dimStyle.Render("No task available."))
			return
		}
		s.fail(err)
		return
	}
	s.lastTaskQueue = queue
	s.lastTaskID = task.ID
	fmt.Println(renderTaskCard(task))
}

func (s *replSession) handleAck(args []string) {
	ref, id, _, ok := s.taskRef(args)
	if !ok {
		s.usage("ack [queue] <task|json|id>")
		return
	}
	queue := s.queueFor(args)
	if err := command.TaskAck(s.ctx, s.client, queue, ref); err != nil {
		s.fail(err)
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf("● Acknowledged task %s", shortID(id))))
}

func (s *replSession) handleNack(args []string) {
	var retryAfter time.Duration
	var explicit bool
	fs := command.NackFlagSet("nack", &retryAfter, &explicit)
	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		s.usage("nack [queue] <task|json|id> [--retry-after 5s] (" + err.Error() + ")")
		return
	}
	ref, id, _, ok := s.taskRef(rest)
	if !ok {
		s.usage("nack [queue] <task|json|id> [--retry-after 5s]")
		return
	}
	queue := s.queueFor(rest)
	if err := command.TaskNack(s.ctx, s.client, queue, ref, retryAfter, explicit); err != nil {
		s.fail(err)
		return
	}
	fmt.Println(warnStyle.Render(fmt.Sprintf("● Released task %s", shortID(id))))
}

func (s *replSession) handleExtend(args []string) {
	if len(args) < 2 {
		s.usage("extend [queue] <task|json|id> <timeout>")
		return
	}
	timeout, err := time.ParseDuration(args[len(args)-1])
	if err != nil {
		fmt.Println(errStyle.Render("Invalid timeout duration"))
		return
	}
	ref, id, _, ok := s.taskRef(args[:len(args)-1])
	if !ok {
		s.usage("extend [queue] <task|json|id> <timeout>")
		return
	}
	queue := s.queueFor(args[:len(args)-1])
	if err := command.TaskExtend(s.ctx, s.client, queue, ref, timeout); err != nil {
		s.fail(err)
		return
	}
	fmt.Println(infoStyle.Render(fmt.Sprintf("● Extended lease for task %s", shortID(id))))
}

func (s *replSession) handleDLQ(args []string) {
	queue, _, err := s.resolveQueue(args, 0)
	if err != nil {
		s.usage("dlq [queue]")
		return
	}
	tasks, err := command.TaskDeadLetters(s.ctx, s.client, queue)
	if err != nil {
		s.fail(err)
		return
	}
	if len(tasks) == 0 {
		fmt.Println(dimStyle.Render("No dead-letter tasks."))
		return
	}
	for _, task := range tasks {
		fmt.Println(renderDeadLetterCard(task))
	}
}

func (s *replSession) handleDLQAck(args []string) {
	ref, id, _, ok := s.taskRef(args)
	if !ok {
		s.usage("dlq-ack [queue] <task|json|id>")
		return
	}
	queue := s.queueFor(args)
	if err := command.TaskAckDeadLetter(s.ctx, s.client, queue, ref); err != nil {
		s.fail(err)
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf("● Removed dead-letter task %s", shortID(id))))
}

func (s *replSession) handleArchive(args []string) {
	ref, id, _, ok := s.taskRef(args)
	if !ok {
		s.usage("archive [queue] <task|json|id>")
		return
	}
	queue := s.queueFor(args)
	if err := command.TaskArchive(s.ctx, s.client, queue, ref); err != nil {
		s.fail(err)
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf("● Archived task %s", shortID(id))))
}

func renderTaskCard(task mikyu.Task) string {
	lease := "pending"
	leaseStyle := taskPendingStyle
	if !task.Visibility.IsZero() {
		d := time.Until(task.Visibility)
		if d <= 0 {
			lease = "expired"
			leaseStyle = taskExpiredStyle
		} else {
			lease = fmt.Sprintf("%s remaining", d.Round(time.Second))
			leaseStyle = taskLeaseStyle
		}
	}
	payload := command.FormatSingleLinePayload(task.Payload, 72)
	content := fmt.Sprintf(
		"%s\n%s\n%s\n%s",
		taskKeyStyle.Render("ID:       ")+taskValueStyle.Render(task.ID),
		taskKeyStyle.Render("Attempts: ")+taskValueStyle.Render(fmt.Sprintf("%d / %d", task.Attempts, task.MaxAttempts)),
		taskKeyStyle.Render("Lease:    ")+leaseStyle.Render(lease),
		taskKeyStyle.Render("Payload:  ")+taskValueStyle.Render(payload),
	)
	return taskCardBorderStyle.Render(content)
}

func renderDeadLetterCard(task mikyu.Task) string {
	payload := command.FormatSingleLinePayload(task.Payload, 72)
	content := fmt.Sprintf(
		"%s\n%s\n%s\n%s",
		taskKeyStyle.Render("ID:       ")+taskValueStyle.Render(task.ID),
		taskKeyStyle.Render("Attempts: ")+taskValueStyle.Render(fmt.Sprintf("%d / %d", task.Attempts, task.MaxAttempts)),
		taskKeyStyle.Render("State:    ")+taskDeadStyle.Render("dead-letter"),
		taskKeyStyle.Render("Payload:  ")+taskValueStyle.Render(payload),
	)
	return taskCardBorderStyle.Render(content)
}
