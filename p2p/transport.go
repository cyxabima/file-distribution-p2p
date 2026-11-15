package p2p

import (
	"net"
)

// Peer is an interface that represents the remote node.
type Peer interface {
	Send([]byte) error
	Receive() ([]byte, error)
	RemoteAddr() net.Addr
	Close() error
}

// Transport is anything that handles the communication between the nodes in a
// network. This can be TCP, UDP, WebSockets, ...
type Transport interface {
	ListenAndAccept() error
	Dial(string) error
	Consume() <-chan Message
	SetOnPeer(func(Peer) error)
	SetOnPeerDisconnect(func(net.Addr))
	Addr() net.Addr
	Close() error
	// Wait blocks until Close has finished tearing the transport down.
	Wait()
}
