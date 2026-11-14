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

func ProtocolHandshake(peer p2p.Peer) error {
	dec := p2p.LengthDecoder{}
	if err := peer.Send(MagicProtocolString); err != nil {
		return err
	}

	var msg p2p.Message
	if err := dec.Decode(peer, &msg); err != nil {
		return err
	}

	if !bytes.Equal(msg.Payload, MagicProtocolString) {
		return fmt.Errorf("invalid protocol handshake from %s", peer.RemoteAddr())
	}

	return nil
}
