package p2p

// HandshakeFunc runs once per connection, before the peer is handed to the
// application. The transport sets a read/write deadline around it, so an
// implementation may read and write freely without risking a stuck goroutine.
type HandshakeFunc func(Peer) error

// NOPHandshakeFunc accepts every peer without exchanging anything.
func NOPHandshakeFunc(Peer) error {
	return nil
}
