package p2p

import "net"

// Peer is an interface that represent the remote node
type Peer interface{
	Send([]byte) error 
	RemoteAddr() net.Addr
	Close() error
}

// Transport is anything that handles the communication
// between the nodes in network. this can be of the 
// from (TCP, UDP webSockets, ...)
type Transport interface{
	ListenAndAccpet() error
	Dial(string) error
	OnPeer (Peer) error // a function for doing processing on peer
	Consume() <- chan Message
	SetOnPeer(func(Peer) error)
}
