package main

import (
	"encoding/binary"
	"fmt"

	"github.com/cyxabima/minimal-p2p-library-in-go/p2p"
)

// handleBitfield processes the initial state dump from a new peer
func (app *FileShareApp) handleBitfield(peer p2p.Peer, data []byte) error {
	peerBf := &Bitfield{
		data: make([]byte, len(data)),
	}
	copy(peerBf.data, data)
	app.state.SetPeerBitfields(peer.RemoteAddr(), peerBf)

	if !app.state.HasSentBitfield(peer.RemoteAddr()) {
		return app.sendBitfield(peer)
	}

	return nil
}

// handleHave processes the request to update Bitfield after broadcast
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

// handleRequest processes the request for actual chunk
func (app *FileShareApp) handleRequest(peer p2p.Peer, data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("invalid MsgRequest payload")
	}
	chunkIndex := binary.BigEndian.Uint32(data)

	if !app.state.MyBitfield.Has(chunkIndex) {
		return nil
	}

	chunkData, err := app.disk.ReadChunk(chunkIndex)

	if err != nil {
		return fmt.Errorf("disk read failed: %v", err)
	}

	// Construct and send MsgPiece 1 byte for type 4 byte for chunkIndex
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

	app.state.MyBitfield.Set(chunkIndex)
	app.state.ClearInFlight(chunkIndex)

	// Most Import broadcast to eveyone that i have now this chuck
	havePayload := make([]byte, 5)
	havePayload[0] = MsgHave
	binary.BigEndian.PutUint32(havePayload[1:], chunkIndex)
	app.swarm.Broadcast(havePayload)

	return nil
}

// TODO: Mueed Bhai
func (app *FileShareApp) RequestBlueprint(peer p2p.Peer) error
func (app *FileShareApp) handleGetBlueprint(peer p2p.Peer) error
func (app *FileShareApp) handleBlueprint(peer p2p.Peer, rawJSON []byte) error


func (app *FileShareApp) sendBitfield(peer p2p.Peer) error {
	app.mu.RLock()
	defer app.mu.RUnlock()

	// TODO: GURAD if we havenot loaded a .p2p blueprint yet, we have no bitfield to send
	if app.state == nil {
		return nil
	}

	app.state.MyBitfield.mu.RLock()
	bfBytes := make([]byte, len(app.state.MyBitfield.data))
	copy(bfBytes, app.state.MyBitfield.data)
	app.state.MyBitfield.mu.RUnlock()

	payload := make([]byte, 1+len(bfBytes))
	payload[0] = MsgBitfield
	copy(payload[1:], bfBytes)

	app.state.MarkBitfieldSent(peer.RemoteAddr())

	return peer.Send(payload)
}

