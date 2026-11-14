package p2p

import (
	"io"
	"net"
)

// Peer is an interface that represent the remote node
type Peer interface{
	io.Reader
	Send([]byte) error 
	RemoteAddr() net.Addr
	Close() error
}

// Transport is anything that handles the communication
// between the nodes in network. this can be of the 
// from (TCP, UDP webSockets, ...)
type Transport interface{
	ListenAndAccept() error
	Dial(string) error
	Consume() <- chan Message
	SetOnPeer(func(Peer) error)
}
