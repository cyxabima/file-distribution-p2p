package p2p

import "net"

// Message holds any arbitary data thai is being sent over
// each transport between two nodes in the network
type Message struct{
 	From net.Addr 
	Payload []byte
	Peer Peer  // NOTE: either we can put the peer on Message get every time from swarm manger for the application but we need to lock and unlock again and again it will decrease performance  
}
