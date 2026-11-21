package p2p

import (
	"errors"
	"net"
	"sync"
)

type SwarmManagerOpts struct {
	Transport Transport
	OnMessage func(Message) error
	// These callbacks let the application do whatever it wants with peers.
	OnPeerConnect    func(Peer) error
	OnPeerDisconnect func(net.Addr)

	// Logger receives swarm diagnostics. Defaults to log.Default().
	Logger Logger
}

type SwarmManager struct {
	SwarmManagerOpts
	peerLock sync.RWMutex
	peers    map[net.Addr]Peer

	// loggerMu guards Logger because SetLogger may be called after Start.
	loggerMu sync.RWMutex
}

func NewSwarmManager(opts SwarmManagerOpts) (*SwarmManager, error) {
	if opts.Transport == nil {
		return nil, errors.New("p2p: SwarmManager requires a Transport")
	}
	opts.Logger = loggerOr(opts.Logger)

	sw := &SwarmManager{
		SwarmManagerOpts: opts,
		peers:            make(map[net.Addr]Peer),
	}

	sw.Transport.SetOnPeer(sw.handleNewPeer)
	sw.Transport.SetOnPeerDisconnect(sw.handlePeerDisconnect)

	return sw, nil
}

// logf snapshots the current logger under a lock.
func (sm *SwarmManager) logf(format string, args ...any) {
	sm.loggerMu.RLock()
	l := sm.Logger
	sm.loggerMu.RUnlock()
	l.Printf(format, args...)
}

// SetLogger redirects swarm diagnostics and, when the transport supports it,
// the transport's diagnostics too. It is safe to call after Start.
func (sm *SwarmManager) SetLogger(l Logger) {
	l = loggerOr(l)

	sm.loggerMu.Lock()
	sm.Logger = l
	sm.loggerMu.Unlock()

	if ls, ok := sm.Transport.(interface{ SetLogger(Logger) }); ok {
		ls.SetLogger(l)
	}
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
	if call(sm.logf, "OnPeerConnect", func() { err = sm.OnPeerConnect(peer) }) {
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
		call(sm.logf, "OnPeerDisconnect", func() { sm.OnPeerDisconnect(addr) })
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
				sm.logf("p2p: broadcast to %s failed: %v", peer.RemoteAddr(), err)
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
		if call(sm.logf, "OnMessage", func() { err = sm.OnMessage(msg) }) {
			// Already logged with a stack trace; keep the swarm alive.
			continue
		}

		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			sm.logf("p2p: message from %s failed: %v", msg.From, err)
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
