package main

import (
	"encoding/binary"
	"errors"
	"fmt"
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
	eventBufferSize     = 256
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

	// logger is injectable so a TUI can keep diagnostics off the terminal.
	logger   Logger
	loggerMu sync.RWMutex

	// stopCh ends the request engine; wg joins the app-owned goroutines.
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// events is a non-blocking progress stream for a UI. A slow consumer drops
	// events rather than stalling downloads.
	events     chan Event
	eventsMu   sync.RWMutex
	eventsDone bool
	eventsOnce sync.Once

	// downloadDone guards the one-shot DownloadComplete event per session.
	downloadDone bool

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
		logger:           loggerOr(nil),
		stopCh:           make(chan struct{}),
		events:           make(chan Event, eventBufferSize),
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

// SetLogger redirects diagnostics from the app, swarm and transport. Passing
// nil restores the default stderr logger. Call it before Start for CLI use; it
// is also safe afterwards because every logger read is locked.
func (app *FileShareApp) SetLogger(l Logger) {
	l = loggerOr(l)

	app.loggerMu.Lock()
	app.logger = l
	app.loggerMu.Unlock()

	app.swarm.SetLogger(l)
}

func (app *FileShareApp) logf(format string, args ...any) {
	app.loggerMu.RLock()
	l := app.logger
	app.loggerMu.RUnlock()
	l.Printf(format, args...)
}

// Events returns the read side of the progress stream. It is closed by Wait.
func (app *FileShareApp) Events() <-chan Event {
	return app.events
}

// emit never blocks the data path: a slow or absent UI drops events instead
// of holding up chunk handling.
func (app *FileShareApp) emit(e Event) {
	app.eventsMu.RLock()
	defer app.eventsMu.RUnlock()
	if app.eventsDone {
		return
	}
	select {
	case app.events <- e:
	default:
	}
}

func (app *FileShareApp) closeEvents() {
	app.eventsOnce.Do(func() {
		app.eventsMu.Lock()
		defer app.eventsMu.Unlock()
		app.eventsDone = true
		close(app.events)
	})
}

// Stop ends the request engine and closes the swarm. It is non-blocking and
// idempotent, so a UI can call it from any goroutine. Use Wait to join.
func (app *FileShareApp) Stop() error {
	app.stopOnce.Do(func() { close(app.stopCh) })
	return app.swarm.Stop()
}

// Wait blocks until Stop has been called and every app goroutine has exited,
// then closes the Events channel. Calling Wait without Stop blocks forever.
func (app *FileShareApp) Wait() {
	app.wg.Wait()
	app.swarm.Wait()
	app.closeEvents()
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
	if _, err := app.setBlueprint(b, disk, true, false, ""); err != nil {
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
func (app *FileShareApp) setBlueprint(b *Blueprint, disk *DiskManger, haveAll, onlyIfEmpty bool, from string) (bool, error) {
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
	app.downloadDone = false
	app.mu.Unlock()

	// Do not hold app.mu while closing: the disk lock is a different lock.
	if old != nil {
		_ = old.Close()
	}

	app.emit(Event{
		Type:        BlueprintLoaded,
		InfoHash:    b.InfoHash,
		FileName:    b.FileName,
		PeerAddr:    from,
		ChunksTotal: totalChunks,
	})
	return true, nil
}

func (app *FileShareApp) Start() {
	app.wg.Add(2)

	// Start the network listening loop.
	go func() {
		defer app.wg.Done()
		err := app.swarm.Start()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			app.logf("file share app: swarm stopped: %v", err)
			app.emit(Event{Type: Error, Err: err})
		}
	}()

	// Start the background download strategy loop. It waits until a
	// blueprint is loaded, so it is safe to start before seeding/fetching.
	go func() {
		defer app.wg.Done()
		app.StartRequestEngine()
	}()
}

// peerAddrString is nil-safe so tests and early callbacks can still emit.
func peerAddrString(peer p2p.Peer) string {
	if peer == nil {
		return ""
	}
	if addr := peer.RemoteAddr(); addr != nil {
		return addr.String()
	}
	return ""
}

// onMessage is called by the SwarmManager whenever a complete message arrives.
func (app *FileShareApp) onMessage(msg p2p.Message) error {
	if len(msg.Payload) == 0 {
		return nil
	}

	msgID := msg.Payload[0]
	data := msg.Payload[1:]

	var err error
	switch msgID {
	case MsgBitfield:
		err = app.handleBitfield(msg.Peer, data)
	case MsgHave:
		err = app.handleHave(msg.Peer, data)
	case MsgRequest:
		err = app.handleRequest(msg.Peer, data)
	case MsgPiece:
		err = app.handlePiece(msg.Peer, data)
	case MsgGetBlueprint:
		err = app.handleGetBlueprint(msg.Peer)
	case MsgBlueprint:
		err = app.handleBlueprint(msg.Peer, data)
	default:
		err = fmt.Errorf("unknown message ID: %x", msgID)
	}

	if err != nil {
		infoHash, fileName := fileInfo(app.session())
		app.emit(Event{
			Type:     Error,
			InfoHash: infoHash,
			FileName: fileName,
			PeerAddr: peerAddrString(msg.Peer),
			Err:      err,
		})
	}
	return err
}

func (app *FileShareApp) OnPeerConnect(peer p2p.Peer) error {
	sess := app.session()
	infoHash, fileName := fileInfo(sess)
	app.emit(Event{
		Type:     PeerConnected,
		InfoHash: infoHash,
		FileName: fileName,
		PeerAddr: peerAddrString(peer),
	})

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
	infoHash, fileName := fileInfo(sess)
	peerAddr := ""
	if addr != nil {
		peerAddr = addr.String()
	}

	if sess.state != nil {
		sess.state.RemovePeer(addr)
	}

	app.mu.Lock()
	delete(app.pendingBlueprint, addr)
	app.mu.Unlock()

	app.emit(Event{
		Type:     PeerDisconnected,
		InfoHash: infoHash,
		FileName: fileName,
		PeerAddr: peerAddr,
	})
}

func (app *FileShareApp) StartRequestEngine() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-app.stopCh:
			return
		case <-ticker.C:
		}

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
