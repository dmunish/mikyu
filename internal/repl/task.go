package repl

import (
	"errors"
	"fmt"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/command"
)

func shortID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

func (s *replSession) resolveQueue(args []string, minAfter int) (string, []string, error) {
	if s.activeQueue != "" && len(args) <= minAfter {
		return s.activeQueue, args, nil
	}

	if len(args) == 0 {
		return "", nil, fmt.Errorf("queue required (or use 'use <queue>')")
	}

	return args[0], args[1:], nil
}

func (s *replSession) handlePublish(args []string) {
	var err error

	visibility, args, err := command.ExtractDuration(args, "--visibility", 0)
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	maxAttempts, args, err := command.ExtractUint32(args, "--max-attempts", 0)
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	at, args, err := command.ExtractTime(args, "--at", time.Time{})
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	queue, rest, err := s.resolveQueue(args, 1)
	if err != nil {
		fmt.Println(errStyle.Render("Usage: publish [queue] <payload|@file|-> [--visibility 30s] [--max-attempts 5]"))
		return
	}

	if len(rest) < 1 {
		fmt.Println(errStyle.Render("Usage: publish [queue] <payload|@file|-> [--visibility 30s] [--max-attempts 5]"))
		return
	}

	task, n, err := command.Publish(s.ctx, s.client, queue, rest[0], command.PublishOptions{
		VisibilityTimeout: visibility,
		MaxAttempts:       maxAttempts,
		ScheduleAt:        at,
	})
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	fmt.Println(successStyle.Render(fmt.Sprintf(
		"● Published task %s to %q (%d bytes)",
		shortID(task.ID),
		queue,
		n,
	)))
}

func (s *replSession) handlePop(args []string) {
	timeout, args, err := command.ExtractDuration(args, "--timeout", 30*time.Second)
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	queue, _, err := s.resolveQueue(args, 0)
	if err != nil {
		fmt.Println(errStyle.Render("Usage: pop [queue] [--timeout 30s]"))
		return
	}

	task, err := command.Pop(s.ctx, s.client, queue, timeout)
	if err != nil {
		if errors.Is(err, mikyu.ErrTaskNotAvailable) {
			fmt.Println(dimStyle.Render("No task available."))
			return
		}

		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	s.lastTaskQueue = queue
	s.lastTaskID = task.ID

	fmt.Println(renderTaskCard(task))
}

func (s *replSession) handleAck(args []string) {
	var queue string
	var taskID string

	if len(args) == 0 && s.lastTaskID != "" {
		queue = s.lastTaskQueue
		taskID = s.lastTaskID
	} else {
		var rest []string
		var err error

		queue, rest, err = s.resolveQueue(args, 1)
		if err != nil || len(rest) < 1 {
			fmt.Println(errStyle.Render("Usage: ack [queue] <task_id>"))
			return
		}

		taskID = rest[0]
	}

	if err := command.TaskAck(s.ctx, s.client, queue, taskID); err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	fmt.Println(successStyle.Render(fmt.Sprintf("● Acknowledged task %s", shortID(taskID))))
}

func (s *replSession) handleNack(args []string) {
	retryAfter, explicit, args, err := command.ExtractOptionalDuration(args, "--retry-after")
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	var queue string
	var taskID string

	if len(args) == 0 && s.lastTaskID != "" {
		queue = s.lastTaskQueue
		taskID = s.lastTaskID
	} else {
		var rest []string

		queue, rest, err = s.resolveQueue(args, 1)
		if err != nil || len(rest) < 1 {
			fmt.Println(errStyle.Render("Usage: nack [queue] <task_id> [--retry-after 5s]"))
			return
		}

		taskID = rest[0]
	}

	if err := command.TaskNack(s.ctx, s.client, queue, taskID, retryAfter, explicit); err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	fmt.Println(warnStyle.Render(fmt.Sprintf("● Released task %s", shortID(taskID))))
}

func (s *replSession) handleExtend(args []string) {
	timeout, explicit, args, err := command.ExtractOptionalDuration(args, "--timeout")
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	var queue string
	var taskID string

	if len(args) == 0 && s.lastTaskID != "" {
		queue = s.lastTaskQueue
		taskID = s.lastTaskID
	} else {
		var rest []string

		queue, rest, err = s.resolveQueue(args, 1)
		if err != nil || len(rest) < 1 {
			fmt.Println(errStyle.Render("Usage: extend [queue] <task_id> <timeout>"))
			return
		}

		taskID = rest[0]
		args = rest[1:]
	}

	if !explicit && len(args) > 0 {
		timeout, err = time.ParseDuration(args[0])
		if err != nil {
			fmt.Println(errStyle.Render("Invalid timeout duration"))
			return
		}
	}

	if timeout <= 0 {
		fmt.Println(errStyle.Render("Usage: extend [queue] <task_id> <timeout>"))
		return
	}

	if err := command.TaskExtend(s.ctx, s.client, queue, taskID, timeout); err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	fmt.Println(infoStyle.Render(fmt.Sprintf("● Extended lease for task %s", shortID(taskID))))
}

func (s *replSession) handleDLQ(args []string) {
	queue, _, err := s.resolveQueue(args, 0)
	if err != nil {
		fmt.Println(errStyle.Render("Usage: dlq [queue]"))
		return
	}

	tasks, err := command.TaskDeadLetters(s.ctx, s.client, queue)
	if err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
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
	queue, rest, err := s.resolveQueue(args, 1)
	if err != nil || len(rest) < 1 {
		fmt.Println(errStyle.Render("Usage: dlq-ack [queue] <task_id>"))
		return
	}

	taskID := rest[0]

	if err := command.TaskAckDeadLetter(s.ctx, s.client, queue, taskID); err != nil {
		fmt.Println(errStyle.Render("Error: " + err.Error()))
		return
	}

	fmt.Println(successStyle.Render(fmt.Sprintf("● Removed dead-letter task %s", shortID(taskID))))
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
