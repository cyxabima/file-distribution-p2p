package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
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

func ProtocolHandshake(conn net.Conn) error {
	if _, err := conn.Write(MagicProtocolString); err != nil {
		return err
	}

	peerMagic := make([]byte, len(MagicProtocolString))
	if _, err := io.ReadFull(conn, peerMagic); err != nil {
		return err
	}

	if !bytes.Equal(peerMagic, MagicProtocolString) {
		return fmt.Errorf("invalid handshake: unrecognized protocol")
	}

	return nil
}
