package p2p

import (
	"encoding/binary"
	"encoding/gob"
	"io"
)

type Decoder interface {
	Decode(io.Reader, *Message) error
}

type Encoder interface{
	Encode(io.Writer, []byte) error
}

type  GOBDecoder struct{}

func (dec GOBDecoder) Decode( r io.Reader, msg *Message) error {
	return  gob.NewDecoder(r).Decode(msg)
}

type DefaultDecoder struct{}

func (dec DefaultDecoder) Decode(r io.Reader, msg *Message) error{

	buff := make([]byte, 1028)
	n, err := r.Read(buff)
	if err != nil{
		return err
	}

	msg.Payload = buff[:n]
	return  nil

}


type DefaultEncoder struct{}

func (e DefaultEncoder) Encode(w io.Writer, payload []byte) error {
	// Write the 4-byte length prefix
	length := uint32(len(payload))
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return err
	}
	
	// Write the actual raw payload
	_, err := w.Write(payload)
	return err
}


type LengthEncoder struct{}

func (e LengthEncoder) Encode(w io.Writer, payload []byte) error {
	payloadLen := uint32(len(payload))

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, payloadLen)

	if _, err := w.Write(lenBuf); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}


type LengthDecoder struct{}

func (d LengthDecoder) Decode(r io.Reader, msg *Message) error {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf)

	// NOTE: we may add a max size check here to prevent memory exhaustion attacks but for now skipping
	msg.Payload = make([]byte, payloadLen)

	_, err := io.ReadFull(r, msg.Payload)
	return err
}
