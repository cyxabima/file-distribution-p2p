package main

import (
	"net"
	"time"
)

type PendingRequest struct {
	PeerAddr    net.Addr
	RequestedAt time.Time
}
