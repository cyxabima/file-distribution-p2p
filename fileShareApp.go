package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cyxabima/minimal-p2p-library-in-go/p2p"
)

const (
	defaultStorageDir   = "p2p_data"
	blueprintRequestTTL = 30 * time.Second
)

type FileShareApp struct {
	listenAddr string
	storageDir string
	swarm      *p2p.SwarmManager
	state      *ApplicationState
	disk       *DiskManger
	verifier   *DataVerifier
	blueprint  *Blueprint

	// pendingBlueprint remembers who we asked for a blueprint, so a random
	// peer cannot push one on us. The TTL stops the map growing forever.
	pendingBlueprint map[net.Addr]time.Time

	mu sync.RWMutex
}

// fileSession is a consistent snapshot of the fields that change when a
// blueprint is loaded. Handlers use it so a late blueprint cannot race with
// messages that are already arriving from other peers.
type fileSession struct {
	state     *ApplicationState
	disk      *DiskManger
	verifier  *DataVerifier
	blueprint *Blueprint
}

func (app *FileShareApp) session() fileSession {
	app.mu.RLock()
	defer app.mu.RUnlock()

	return fileSession{
		state:     app.state,
		disk:      app.disk,
		verifier:  app.verifier,
		blueprint: app.blueprint,
	}
}

func NewFileShareApp(listenAddr, storageDir string) (*FileShareApp, error) {
	if storageDir == "" {
		storageDir = defaultStorageDir
	}

	app := FileShareApp{
		listenAddr:       listenAddr,
		storageDir:       storageDir,
		pendingBlueprint: make(map[net.Addr]time.Time),
	}

	// Encoder/Decoder are left unset on purpose as our p2p lib has the built-in length-prefixed
	transport, err := p2p.NewTCPTransport(p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: ProtocolHandshake,
	})
	if err != nil {
		return nil, err
	}

	swarm, err := p2p.NewSwarmManager(p2p.SwarmManagerOpts{
		Transport:        transport,
		OnMessage:        app.onMessage, // for wiring incoming network
		OnPeerConnect:    app.OnPeerConnect,
		OnPeerDisconnect: app.OnPeerDisconnect,
	})
	if err != nil {
		return nil, err
	}
	app.swarm = swarm

	return &app, nil
}

// LoadSeedFile hashes an existing file, saves its blueprint, and makes this
// app a seeder for it. The source file is opened read-only: os.Create would
// truncate the very file we are sharing.
func (app *FileShareApp) LoadSeedFile(path string, chunkSize uint32) (*Blueprint, error) {
	b, err := BuildBlueprint(path, chunkSize)
	if err != nil {
		return nil, err
	}

	disk, err := OpenDiskManger(path, b.FileSize, b.ChunkSize)
	if err != nil {
		return nil, err
	}
	if err := SaveBlueprint(app.storageDir, b); err != nil {
		_ = disk.Close()
		return nil, err
	}
	if _, err := app.setBlueprint(b, disk, true, false); err != nil {
		_ = disk.Close()
		return nil, err
	}

	return b, nil
}

// downloadPath keeps a peer-supplied file name inside storageDir/downloads.
func (app *FileShareApp) downloadPath(b *Blueprint) (string, error) {
	name, err := safeFileName(b.FileName)
	if err != nil {
		return "", err
	}

	dir := filepath.Join(app.storageDir, "downloads")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return filepath.Join(dir, name), nil
}

// setBlueprint installs the single-file session. haveAll is true for a seeder
// (it owns every chunk) and false for a leecher that just got a blueprint.
// When onlyIfEmpty is set, the whole check-and-install is atomic so two peers
// answering a blueprint request cannot both install a different file.
func (app *FileShareApp) setBlueprint(b *Blueprint, disk *DiskManger, haveAll, onlyIfEmpty bool) (bool, error) {
	if err := ValidateBlueprint(b); err != nil {
		return false, err
	}

	totalChunks := uint32((b.FileSize + int64(b.ChunkSize) - 1) / int64(b.ChunkSize))
	state := NewApplicationState(totalChunks)
	if haveAll {
		for i := uint32(0); i < totalChunks; i++ {
			state.MyBitfield.Set(i)
		}
	}

	app.mu.Lock()
	if onlyIfEmpty && app.blueprint != nil {
		app.mu.Unlock()
		return false, nil
	}
	old := app.disk
	app.blueprint = b
	app.state = state
	app.verifier = NewDataVerifier(b.Hashes)
	app.disk = disk
	app.mu.Unlock()

	// Do not hold app.mu while closing: the disk lock is a different lock.
	if old != nil {
		_ = old.Close()
	}
	return true, nil
}

func (app *FileShareApp) Start() {
	// Start the network listening loop
	go func() {
		if err := app.swarm.Start(); err != nil {
			log.Printf("file share app: swarm stopped: %v", err)
		}
	}()

	// Start the background download strategy loop. It waits until a
	// blueprint is loaded, so it is safe to start before seeding/fetching.
	go app.StartRequestEngine()
}

// OnMessage is called by the SwarmManager whenever a complete message arrives
func (app *FileShareApp) onMessage(msg p2p.Message) error {
	if len(msg.Payload) == 0 {
		return nil
	}

	msgID := msg.Payload[0]
	data := msg.Payload[1:]

	switch msgID {
	case MsgBitfield:
		return app.handleBitfield(msg.Peer, data)
	case MsgHave:
		return app.handleHave(msg.Peer, data)
	case MsgRequest:
		return app.handleRequest(msg.Peer, data)
	case MsgPiece:
		return app.handlePiece(msg.Peer, data)
	case MsgGetBlueprint:
		return app.handleGetBlueprint(msg.Peer)
	case MsgBlueprint:
		return app.handleBlueprint(msg.Peer, data)
	default:
		return fmt.Errorf("unknown message ID: %x", msgID)
	}
}

func (app *FileShareApp) OnPeerConnect(peer p2p.Peer) error {
	sess := app.session()

	// A fresh leecher has no bitfield to send yet. Ask this peer for the
	// blueprint; handleBlueprint will send our bitfield once it arrives.
	if sess.state == nil {
		return app.RequestBlueprint(peer)
	}

	return app.sendBitfield(peer)
}

// we need this as swarm manager can not update app state
func (app *FileShareApp) OnPeerDisconnect(addr net.Addr) {
	sess := app.session()
	if sess.state != nil {
		sess.state.RemovePeer(addr)
	}

	app.mu.Lock()
	delete(app.pendingBlueprint, addr)
	app.mu.Unlock()
}

func (app *FileShareApp) StartRequestEngine() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		sess := app.session()
		if sess.state == nil || sess.verifier == nil || sess.disk == nil {
			continue // no blueprint yet: wait for LoadSeedFile or handleBlueprint
		}

		// Derive this every tick: the blueprint can arrive after Start().
		totalChunks := uint32(len(sess.verifier.expectedHashes))
		sess.state.SweepTimeouts(5 * time.Second)

		// scan the applicationState for the rarest chunk
		chunkIndex, peerAddr, found := sess.state.FindNextRequest(totalChunks)

		if !found {
			continue
		}

		peer, exists := app.swarm.GetPeer(peerAddr)
		if !exists {
			continue
		}

		sess.state.MarkInFlight(chunkIndex, peerAddr)

		payload := make([]byte, 5)
		payload[0] = MsgRequest
		binary.BigEndian.PutUint32(payload[1:], chunkIndex)

		peer.Send(payload)
	}
}

// NOTE : Refactoring Requied as we need to handle multiple file sharing as once for that we need each file blueprint hash as well for session or other things ig
// In V1 we are supporting one file at a time this will help us to keep away from session managmnet now
