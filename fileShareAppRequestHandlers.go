package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/cyxabima/minimal-p2p-library-in-go/p2p"
)

// handleBitfield processes the initial state dump from a new peer
func (app *FileShareApp) handleBitfield(peer p2p.Peer, data []byte) error {
	sess := app.session()
	if sess.state == nil || sess.verifier == nil {
		// A peer can send its bitfield before our blueprint arrives. Ignore
		// it; handleGetBlueprint asks it to send the bitfield again after the
		// blueprint, and TCP keeps those two messages in order.
		return nil
	}

	// A short or long bitfield would make every later chunk lookup wrong.
	totalChunks := uint32(len(sess.verifier.expectedHashes))
	want := (totalChunks + 7) / 8
	if uint32(len(data)) != want {
		return fmt.Errorf("invalid bitfield length: got %d bytes, want %d", len(data), want)
	}

	peerBf := &Bitfield{
		data: append([]byte(nil), data...),
	}
	sess.state.SetPeerBitfields(peer.RemoteAddr(), peerBf)

	if !sess.state.HasSentBitfield(peer.RemoteAddr()) {
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

	sess := app.session()
	if sess.state == nil {
		return nil // no blueprint yet
	}

	// Update through the state object: reaching into its mutex from here
	// bypasses the lock convention and races with the request engine.
	sess.state.UpdatePeerBitfield(peer.RemoteAddr(), chunkIndex)

	return nil
}

// handleRequest processes the request for actual chunk
func (app *FileShareApp) handleRequest(peer p2p.Peer, data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("invalid MsgRequest payload")
	}
	chunkIndex := binary.BigEndian.Uint32(data)

	sess := app.session()
	if sess.state == nil || sess.disk == nil {
		return nil // no blueprint yet
	}

	if !sess.state.MyBitfield.Has(chunkIndex) {
		return nil
	}

	chunkData, err := sess.disk.ReadChunk(chunkIndex)
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

	sess := app.session()
	if sess.state == nil || sess.verifier == nil || sess.disk == nil {
		return nil // no blueprint yet
	}

	if !sess.verifier.Verify(chunkIndex, rawData) {
		sess.state.ClearInFlight(chunkIndex)
		peer.Close() // disconnect the bad actor
		return fmt.Errorf("chunk %d failed SHA-256 verification", chunkIndex)
	}

	if err := sess.disk.WriteChunk(chunkIndex, rawData); err != nil {
		sess.state.ClearInFlight(chunkIndex)
		return fmt.Errorf("disk write failed: %v", err)
	}

	sess.state.MyBitfield.Set(chunkIndex)
	sess.state.ClearInFlight(chunkIndex)

	// Most Import broadcast to eveyone that i have now this chuck
	havePayload := make([]byte, 5)
	havePayload[0] = MsgHave
	binary.BigEndian.PutUint32(havePayload[1:], chunkIndex)
	app.swarm.Broadcast(havePayload)

	return nil
}

// RequestBlueprint asks a peer for the full chunk-hash list.
//
// It must only SEND. peer.Receive() would make us a second reader on the same
// socket as the transport read loop, and the two readers would split frames.
// The reply arrives as an ordinary MsgBlueprint on onMessage.
func (app *FileShareApp) RequestBlueprint(peer p2p.Peer) error {
	app.mu.Lock()
	app.pendingBlueprint[peer.RemoteAddr()] = time.Now()
	app.mu.Unlock()

	return peer.Send([]byte{MsgGetBlueprint})
}

// handleGetBlueprint replies to a blueprint request with our own blueprint.
func (app *FileShareApp) handleGetBlueprint(peer p2p.Peer) error {
	sess := app.session()
	if sess.blueprint == nil {
		return nil // we are also a leecher; nothing to share yet
	}

	encoded, err := json.Marshal(sess.blueprint)
	if err != nil {
		return fmt.Errorf("encode blueprint: %w", err)
	}
	if len(encoded)+1 > int(p2p.DefaultMaxMessageSize) {
		return fmt.Errorf("blueprint too large to send: %d bytes", len(encoded))
	}

	payload := make([]byte, 1+len(encoded))
	payload[0] = MsgBlueprint
	copy(payload[1:], encoded)
	if err := peer.Send(payload); err != nil {
		return err
	}

	// The requester also needs to know which chunks we hold. TCP preserves
	// the order of these two frames, so it will read the blueprint first.
	return app.sendBitfield(peer)
}

// handleBlueprint validates a received blueprint and sets up the leecher side
// of the session: output file, verifier, and an empty bitfield.
func (app *FileShareApp) handleBlueprint(peer p2p.Peer, rawJSON []byte) error {
	if len(rawJSON) == 0 || len(rawJSON)+1 > int(p2p.DefaultMaxMessageSize) {
		return fmt.Errorf("invalid MsgBlueprint payload size: %d", len(rawJSON))
	}

	var b Blueprint
	if err := json.Unmarshal(rawJSON, &b); err != nil {
		return fmt.Errorf("decode blueprint: %w", err)
	}
	if err := ValidateBlueprint(&b); err != nil {
		return err
	}

	addr := peer.RemoteAddr()
	app.mu.Lock()
	requestedAt, asked := app.pendingBlueprint[addr]
	delete(app.pendingBlueprint, addr)
	app.mu.Unlock()

	if !asked || time.Since(requestedAt) > blueprintRequestTTL {
		return fmt.Errorf("unexpected blueprint from %s", addr)
	}
	path, err := app.downloadPath(&b)
	if err != nil {
		return err
	}
	disk, err := NewDiskManger(path, b.FileSize, b.ChunkSize)
	if err != nil {
		return err
	}
	if err := SaveBlueprint(app.storageDir, &b); err != nil {
		_ = disk.Close()
		return err
	}
	installed, err := app.setBlueprint(&b, disk, false, true)
	if err != nil {
		_ = disk.Close()
		return err
	}
	if !installed {
		// Another peer's blueprint won the race; v1 handles one file only.
		_ = disk.Close()
		return nil
	}

	log.Printf("file share app: loaded blueprint %s (%s, %d bytes, %d chunks) from %s",
		b.InfoHash, b.FileName, b.FileSize, len(b.Hashes), addr)

	return app.sendBitfield(peer)
}

func (app *FileShareApp) sendBitfield(peer p2p.Peer) error {
	sess := app.session()

	// GURAD if we havenot loaded a .p2p blueprint yet so we have no bitfield to sen
	if sess.state == nil {
		return nil
	}

	bfBytes := sess.state.MyBitfield.Bytes()

	payload := make([]byte, 1+len(bfBytes))
	payload[0] = MsgBitfield
	copy(payload[1:], bfBytes)

	sess.state.MarkBitfieldSent(peer.RemoteAddr())

	return peer.Send(payload)
}
