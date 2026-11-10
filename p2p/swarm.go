package p2p

import (
	"fmt"
	"net"
	"sync"

)

type SwarmManagerOpts struct {
	Transport Transport
	OnMessage func(Message) error
	
}

type SwarmManager struct{
	SwarmManagerOpts
	peerLock sync.RWMutex
	peers map[net.Addr]Peer
}

func NewSwarmManager(opts SwarmManagerOpts) *SwarmManager {
	sw := &SwarmManager{
		SwarmManagerOpts: opts,
		peers:            make(map[net.Addr]Peer),
	}

	// use this call back to connect SwarmManager to Transport
	sw.Transport.SetOnPeer(sw.handleNewPeer) 

	return sw 
}

func (sm *SwarmManager) handleNewPeer(peer Peer) error{
	sm.peerLock.Lock()
	defer sm.peerLock.Unlock()

	sm.peers[peer.RemoteAddr()] = peer
	fmt.Printf("Swarm added new peer: %s\n", peer.RemoteAddr())
	return nil
}

func (sm *SwarmManager) RemovePeer(addr net.Addr){
	sm.peerLock.Lock()
	defer sm.peerLock.Unlock()

	delete(sm.peers, addr)
}

func (sm *SwarmManager) Broadcast(payload []byte){
	sm.peerLock.RLock()
	defer sm.peerLock.RUnlock()

	for _, peer := range sm.peers {
		go func(peer Peer) {
			if err := peer.Send(payload); err != nil {
			fmt.Printf("Failed to broadcast to %s: %s\n", peer.RemoteAddr(), err)
			//NOTE: we might want to remove the peer from the map here if it drops	
		}
		}(peer)
	}
}

func (sm *SwarmManager) Start(){
	// Start the background listening loop
	go sm.Transport.ListenAndAccept()

	for msg := range sm.Transport.Consume() {
		if err:= sm.OnMessage(msg) ; err != nil {
			fmt.Printf("Swarm: Error processing message from %s: %v\n", msg.From, err)
			if err.Error() == "EOF" {
				sm.RemovePeer(msg.From)
			}
		}
		
	}
}
