package p2p

import (
	"errors"
	"log"
	"net"
	"sync"
)

type SwarmManagerOpts struct {
	Transport Transport
	OnMessage func(Message) error
	// These callbacks let the application do whatever it wants with peers.
	OnPeerConnect    func(Peer) error
	OnPeerDisconnect func(net.Addr)
}

type SwarmManager struct {
	SwarmManagerOpts
	peerLock sync.RWMutex
	peers    map[net.Addr]Peer
}

func NewSwarmManager(opts SwarmManagerOpts) (*SwarmManager, error) {
	if opts.Transport == nil {
		return nil, errors.New("p2p: SwarmManager requires a Transport")
	}

	sw := &SwarmManager{
		SwarmManagerOpts: opts,
		peers:            make(map[net.Addr]Peer),
	}

	sw.Transport.SetOnPeer(sw.handleNewPeer)
	sw.Transport.SetOnPeerDisconnect(sw.handlePeerDisconnect)

	return sw, nil
}

func (sm *SwarmManager) handleNewPeer(peer Peer) error {
	addr := peer.RemoteAddr()

	sm.peerLock.Lock()
	sm.peers[addr] = peer
	sm.peerLock.Unlock()

	if sm.OnPeerConnect == nil {
		return nil
	}

	// Contain a panicking callback here cuz we have to remove the peer from the peer map if onPeerConnect failed
	var err error
	if call("OnPeerConnect", func() { err = sm.OnPeerConnect(peer) }) {
		err = errors.New("p2p: OnPeerConnect panicked")
	}
	if err != nil {
		sm.removePeer(addr)
		return err
	}

	return nil
}

func (sm *SwarmManager) removePeer(addr net.Addr) {
	sm.peerLock.Lock()
	delete(sm.peers, addr)
	sm.peerLock.Unlock()
}

func (sm *SwarmManager) handlePeerDisconnect(addr net.Addr) {
	sm.removePeer(addr)

	if sm.OnPeerDisconnect != nil {
		call("OnPeerDisconnect", func() { sm.OnPeerDisconnect(addr) })
	}
}

func (sm *SwarmManager) Dial(addr string) error {
	return sm.Transport.Dial(addr)
}

func (sm *SwarmManager) GetPeer(addr net.Addr) (Peer, bool) {
	sm.peerLock.RLock()
	defer sm.peerLock.RUnlock()

	peer, ok := sm.peers[addr]
	return peer, ok
}

// RemovePeer detaches a peer from the swarm and notifies the application.
func (sm *SwarmManager) RemovePeer(addr net.Addr) {
	sm.handlePeerDisconnect(addr)
}


func (sm *SwarmManager) Broadcast(payload []byte) {
	sm.peerLock.RLock()
	peers := make([]Peer, 0, len(sm.peers))
	for _, peer := range sm.peers {
		peers = append(peers, peer)
	}
	sm.peerLock.RUnlock()

	for _, peer := range peers {
		go func(peer Peer) {
			if err := peer.Send(payload); err != nil {
				log.Printf("p2p: broadcast to %s failed: %v", peer.RemoteAddr(), err)
			}
		}(peer)
	}
}

// synchronously consuming the msg channel
func (sm *SwarmManager) Start() error {
	if sm.OnMessage == nil {
		return errors.New("p2p: SwarmManager.OnMessage is nil")
	}

	if err := sm.Transport.ListenAndAccept(); err != nil {
		return err
	}

	for msg := range sm.Transport.Consume() {
		var err error
		if call("OnMessage", func() { err = sm.OnMessage(msg) }) {
			// Already logged with a stack trace; keep the swarm alive.
			continue
		}

		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("p2p: message from %s failed: %v", msg.From, err)
		}
	}

	return nil
}

func (sm *SwarmManager) Stop() error {
	return sm.Transport.Close()
}

// Wait blocks until the transport has fully shut down.
func (sm *SwarmManager) Wait() {
	sm.Transport.Wait()
}
