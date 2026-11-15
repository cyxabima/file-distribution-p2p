package p2p

import "net"

// Message holds any arbitrary data that is being sent over each transport
// between two nodes in the network.
type Message struct {
	From    net.Addr
	Payload []byte
	// Peer is carried on the message so the application does not have to look
	// it up in the swarm and take the lock on every single message.
	Peer Peer
}
