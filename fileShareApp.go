package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cyxabima/minimal-p2p-library-in-go/p2p"
)

type FileShareApp struct {
	listenAddr string
	storageDir string
	swarm    *p2p.SwarmManager
	state    *ApplicationState
	disk     *DiskManger
	verifier *DataVerifier
	mu       sync.RWMutex
}

func NewFileShareApp(listenAddr, storageDir string) (*FileShareApp) {
	app := FileShareApp{
		listenAddr: listenAddr,
		storageDir: storageDir,
	}


	transport := p2p.NewTcpTransport(p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		Encoder:       p2p.LengthEncoder{},
		Decoder:       p2p.LengthDecoder{},
		HandshakeFunc: ProtocolHandshake,
	})

	app.swarm = p2p.NewSwarmManager(p2p.SwarmManagerOpts{
		Transport: transport,
		OnMessage: app.onMessage, // for wiring incoming network
		OnPeerConnect: app.OnPeerConnect,
		OnPeerDisconnect: app.OnPeerDisconnect,
	})

	return &app
}

func (app *FileShareApp) Start() {
	// Start the network listening loop
	go app.swarm.Start()

	// Start the background download strategy loop
	totalChunks := uint32(len(app.verifier.expectedHashes))
	go app.StartRequestEngine(totalChunks)
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
	default:
		return fmt.Errorf("unknown message ID: %x", msgID)
	}
}

func (app *FileShareApp) OnPeerConnect(peer p2p.Peer) error{
	app.mu.RLock()
	state := app.state
	app.mu.RUnlock()

	// If we are a fresh leecher who hasn't downloaded the .p2p blueprint yet,
	// we don't have a bitfield to send yet do nothing as handleBlueprint will send it later.
	if state == nil {
		return nil
	}

	return app.sendBitfield(peer)}

//we need this as swarm manager can not update app state
func (app *FileShareApp) OnPeerDisconnect(addr net.Addr){
	app.mu.RLock()
	state := app.state
	app.mu.RUnlock()
	 if state != nil{
	 state.RemovePeer(addr)
	 }
}

func (app *FileShareApp) StartRequestEngine(totalChunks uint32) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		app.state.SweepTimeouts(5 * time.Second)

		// scan the applicationState for the rarest chunk
		chunkIndex, peerAddr, found := app.state.FindNextRequest(totalChunks)

		if !found {
			continue
		}

		peer, exists := app.swarm.GetPeer(peerAddr)
		if !exists {
			continue
		}

		app.state.MarkInFlight(chunkIndex, peerAddr)

		payload := make([]byte, 5)
		payload[0] = MsgRequest
		binary.BigEndian.PutUint32(payload[1:], chunkIndex)

		peer.Send(payload)
	}
}



// NOTE : Refactoring Requied as we need to handle multiple file sharing as once for that we need each file blueprint hash as well for session or other things ig
// In V1 we are supporting one file at a time this will help us to keep away from session managmnet now
