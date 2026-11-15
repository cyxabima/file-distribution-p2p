package p2p

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const DefaultMaxMessageSize uint32 = 4 << 20 // 4 MiB

var (
	ErrMessageTooLarge = errors.New("p2p: message exceeds max size")
	// ErrKeepAlive is returned by a Decoder for a zero-length frame. It is
	// transport bookkeeping, not an error condition: the read loop resets its
	// deadline and keeps going without emitting a Message.
	ErrKeepAlive = errors.New("p2p: keep-alive frame")
)

type Encoder interface {
	Encode(w io.Writer, payload []byte) error
}

type Decoder interface {
	Decode(r io.Reader) ([]byte, error)
}


type LengthEncoder struct {
	MaxMessageSize uint32
}

func (e LengthEncoder) max() uint32 {
	if e.MaxMessageSize == 0 {
		return DefaultMaxMessageSize
	}
	return e.MaxMessageSize
}

func (e LengthEncoder) Encode(w io.Writer, payload []byte) error {
	if uint32(len(payload)) > e.max() {
		return fmt.Errorf("%w: %d bytes", ErrMessageTooLarge, len(payload))
	}

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}

	_, err := w.Write(payload)
	return err
}

type LengthDecoder struct {
	MaxMessageSize uint32
}

func (d LengthDecoder) max() uint32 {
	if d.MaxMessageSize == 0 {
		return DefaultMaxMessageSize
	}
	return d.MaxMessageSize
}

func (d LengthDecoder) Decode(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}

	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return nil, ErrKeepAlive
	}

	if n > d.max() {
		return nil, fmt.Errorf("%w: %d bytes", ErrMessageTooLarge, n)
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
