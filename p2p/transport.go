package p2p


// Peer is an interface that represent the remote node
type Peer interface{
	Close() error
}

// Transport is anything that handles the communication
// between the nodes in network. this can be of the 
// from (TCP, UDP webSockets, ...)
type Transport interface{
	ListenAndAccpet() error
	Consume() <- chan Message
}
