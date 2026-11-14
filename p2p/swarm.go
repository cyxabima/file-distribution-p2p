package p2p

import (
	"fmt"
	"net"
	"sync"

)

type SwarmManagerOpts struct {
	Transport Transport
	OnMessage func(Message) error
	// these two function so that application can do what ever it wnat
	OnPeerConnect    func(Peer) error
	OnPeerDisconnect func(net.Addr)
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
	sw.Transport.SetOnPeerDisconnect(sw.handlePeerDisconnect)
	return sw
}

func (sm *SwarmManager) handleNewPeer(peer Peer) error{
	sm.peerLock.Lock()
	defer sm.peerLock.Unlock()

	sm.peers[peer.RemoteAddr()] = peer
	fmt.Printf("Swarm added new peer: %s\n", peer.RemoteAddr())
	if sm.OnPeerConnect != nil{
		return sm.OnPeerConnect(peer)
	}
	return nil
}

func (sm *SwarmManager) handlePeerDisconnect(addr net.Addr) {
	sm.peerLock.Lock()
	defer sm.peerLock.Unlock()
	delete(sm.peers, addr)

	if sm.OnPeerDisconnect != nil {
		sm.OnPeerDisconnect(addr)
	}
}

func (sm *SwarmManager) GetPeer(addr net.Addr) (Peer, bool){
	peer, ok := sm.peers[addr]
	if !ok{
		return nil, false
	}
	return peer, true
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
