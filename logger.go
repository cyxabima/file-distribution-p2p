package main

import (
	"log"

	"github.com/cyxabima/minimal-p2p-library-in-go/p2p"
)

// Logger is the shared logging interface. It is an alias so the app, discovery
// and p2p components all accept the same thing: a *log.Logger by default, or a
// discard/file logger when a TUI owns the terminal.
type Logger = p2p.Logger

func loggerOr(l Logger) Logger {
	if l == nil {
		return log.Default()
	}
	return l
}
