package domain

import "errors"

var (
	ErrCycleDetected = errors.New("cycle detected in workflow DAG")
	ErrTransient     = errors.New("transient error, retry required")
	ErrTerminal      = errors.New("terminal error, DLQ required")
)
