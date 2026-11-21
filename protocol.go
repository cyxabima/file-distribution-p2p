package main

import (
	"bytes"
	"fmt"

	"github.com/cyxabima/minimal-p2p-library-in-go/p2p"
)

const (
	MsgBitfield     byte = 0x01
	MsgHave         byte = 0x02
	MsgRequest      byte = 0x03
	MsgPiece        byte = 0x04
	MsgGetBlueprint byte = 0x05
	MsgBlueprint    byte = 0x06
)

var MagicProtocolString = []byte("COMPSTEMS_P2P_V1")

// ProtocolHandshake exchanges the magic string with the remote peer
func ProtocolHandshake(peer p2p.Peer) error {
	if err := peer.Send(MagicProtocolString); err != nil {
		return err
	}

	payload, err := peer.Receive()
	if err != nil {
		return err
	}

	if !bytes.Equal(payload, MagicProtocolString) {
		return fmt.Errorf("invalid protocol handshake from %s", peer.RemoteAddr())
	}

	return nil
}

//  NOTE:
// The handshake is symmetric:
// both sides write their magic first and then read the peers
// Writing first is required, because both nodes run this same function and reading first would make them wait on each other forever
