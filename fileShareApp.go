package main

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cyxabima/minimal-p2p-library-in-go/p2p"
)

type FileShareApp struct {
	swarm    *p2p.SwarmManager
	state    *ApplicationState
	disk     *DiskManger
	verifier *DataVerifier
}

func NewFileShareApp(listenAddr, filepath string, fileSize int64, chuckSize uint32, expectedHashes [][]byte) (*FileShareApp, error) {
	totalChunks := uint32(len(expectedHashes))
	app := FileShareApp{
		state:    NewApplicationState(totalChunks),
		verifier: NewDataVerifier(expectedHashes),
	}

	disk, err := NewDiskManger(filepath, fileSize, chuckSize)
	if err != nil {
		return nil, err
	}

	app.disk = disk

	transport := p2p.NewTcpTransport(p2p.TCPTransportOpts{
		ListenAddr: listenAddr,
		// TODO: Encoder and Decoder functions
	})

	app.swarm = p2p.NewSwarmManager(p2p.SwarmManagerOpts{
		Transport: transport,
		OnMessage: app.onMessage, // for wiring incoming network
	})

	return &app, nil
}

func (app *FileShareApp) Start() {
	// Start the network listening loop
	go app.swarm.Start()

	// Start the background download strategy loop
	totalChunks := uint32(len(app.verifier.expectedHashes))
	go app.StartRequestEngine(totalChunks)
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

		// TODO: Build and send the message Request Payload this need to updated i guess
		payload := make([]byte, 5)
		payload[0] = MsgRequest
		binary.BigEndian.PutUint32(payload[1:], chunkIndex)

		peer.Send(payload)
	}
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

func (app *FileShareApp) handleBitfield(peer p2p.Peer, data []byte) error {
	return nil
}

func (app *FileShareApp) handleHave(peer p2p.Peer, data []byte) error {
	return nil
}

func (app *FileShareApp) handleRequest(peer p2p.Peer, data []byte) error {
	return nil
}

func (app *FileShareApp) handlePiece(peer p2p.Peer, data []byte) error {
	return nil
}
