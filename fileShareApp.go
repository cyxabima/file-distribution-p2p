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
		Encoder: p2p.LengthEncoder{},
		Decoder: p2p.LengthDecoder{},
		HandshakeFunc: ProtocolHandshake,
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

// handleBitfield processes the initial state dump from a new peer
func (app *FileShareApp) handleBitfield(peer p2p.Peer, data []byte) error {
	peerBf := &Bitfield{
		data: make([]byte, len(data)),
	}
	copy(peerBf.data, data)
	app.state.SetPeerBitfields(peer.RemoteAddr(), peerBf)

	return nil
}

func (app *FileShareApp) handleHave(peer p2p.Peer, data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("invalid MsgHave payload")
	}

	chunkIndex := binary.BigEndian.Uint32(data)

	// updating the bitfiled of the peer
	app.state.mu.Lock()
	defer app.state.mu.Unlock()

	if peerBf, exists := app.state.peerBitfields[peer.RemoteAddr()]; exists {
		peerBf.Set(chunkIndex)
	}

	return nil
}

func (app *FileShareApp) handleRequest(peer p2p.Peer, data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("invalid MsgRequest payload")
	}
	chunkIndex := binary.BigEndian.Uint32(data)

	if !app.state.MyBitfiled.Has(chunkIndex){
		return nil
	}

	chunkData, err := app.disk.ReadChunk(chunkIndex)
	
	if err != nil {
		return fmt.Errorf("disk read failed: %v", err)
	}

	// Construct and send MsgPiece
	payload := make([]byte, 1+4+len(chunkData))
	payload[0] = MsgPiece
	binary.BigEndian.PutUint32(payload[1:5], chunkIndex)
	copy(payload[5:], chunkData)

	return peer.Send(payload)
}

// handlePiece processes the actual file data sent by a peer
func (app *FileShareApp) handlePiece(peer p2p.Peer, data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("invalid MsgPiece payload")
	}

	chunkIndex := binary.BigEndian.Uint32(data[:4])
	rawData := data[4:]

	if !app.verifier.Verify(chunkIndex, rawData) {
		app.state.ClearInFlight(chunkIndex)
		peer.Close() // disconnect the bad actor
		return fmt.Errorf("chunk %d failed SHA-256 verification", chunkIndex)
	}

	if err := app.disk.WriteChunk(chunkIndex, rawData); err != nil {
		app.state.ClearInFlight(chunkIndex)
		return fmt.Errorf("disk write failed: %v", err)
	}

	app.state.MyBitfiled.Set(chunkIndex)
	app.state.ClearInFlight(chunkIndex)

	// Most Import broadcast to eveyone that i have now this chuck
	havePayload := make([]byte, 5)
	havePayload[0] = MsgHave
	binary.BigEndian.PutUint32(havePayload[1:], chunkIndex)
	app.swarm.Broadcast(havePayload)

	return nil
}


// NOTE : Refactoring Requied as we need to handle multiple file sharing as once for that we need each file blueprint hash as well for session or other things ig 
// In V1 we are supporting one file at a time this will help us to keep away from session managmnet now 
