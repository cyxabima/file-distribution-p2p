package p2p

import "log"

// Logger is the small logging surface the library needs. *log.Logger satisfies
// it, so callers can route output to a file, a bytes.Buffer, or io.Discard when
// a TUI owns the terminal.
//
// It is safe to change a component's logger while it is running; implementations
// snapshot it under a lock before printing.
type Logger interface {
	Printf(format string, args ...any)
}

// loggerOr keeps the existing CLI behaviour (stderr via log.Default) when no
// logger was supplied.
func loggerOr(l Logger) Logger {
	if l == nil {
		return log.Default()
	}
	return l
}
