package p2p

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startTestTransport builds, configures and listens on an ephemeral port.
// The setup callback runs before ListenAndAccept so callbacks are installed
// before any goroutine can read them.
func startTestTransport(t *testing.T, opts TCPTransportOpts, setup func(*TCPTransport)) *TCPTransport {
	t.Helper()

	if opts.ListenAddr == "" {
		opts.ListenAddr = "127.0.0.1:0"
	}

	tr, err := NewTCPTransport(opts)
	require.NoError(t, err)

	if setup != nil {
		setup(tr)
	}

	require.NoError(t, tr.ListenAndAccept())
	t.Cleanup(func() { _ = tr.Close() })

	return tr
}

func TestMismatchedCodecsRejected(t *testing.T) {
	_, err := NewTCPTransport(TCPTransportOpts{
		ListenAddr: "127.0.0.1:0",
		Encoder:    LengthEncoder{},
	})
	require.ErrorIs(t, err, ErrCodecPair)

	_, err = NewTCPTransport(TCPTransportOpts{
		ListenAddr: "127.0.0.1:0",
		Decoder:    LengthDecoder{},
	})
	require.ErrorIs(t, err, ErrCodecPair)
}

func TestTwoTransportsExchange(t *testing.T) {
	server := startTestTransport(t, TCPTransportOpts{}, nil)

	peerCh := make(chan Peer, 1)
	client, err := NewTCPTransport(TCPTransportOpts{ListenAddr: "127.0.0.1:0"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	client.SetOnPeer(func(p Peer) error {
		peerCh <- p
		return nil
	})
	require.NoError(t, client.ListenAndAccept())
	require.NoError(t, client.Dial(server.Addr().String()))

	var peer Peer
	select {
	case peer = <-peerCh:
	case <-time.After(2 * time.Second):
		t.Fatal("client never reported the peer")
	}

	require.NoError(t, peer.Send([]byte("hello")))

	select {
	case msg := <-server.Consume():
		assert.Equal(t, []byte("hello"), msg.Payload)
		require.NotNil(t, msg.From)
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the message")
	}
}

func TestHandshakeFailureNoDisconnect(t *testing.T) {
	disconnected := make(chan struct{}, 1)

	tr := startTestTransport(t, TCPTransportOpts{
		HandshakeFunc: func(Peer) error { return errors.New("rejected") },
	}, func(tr *TCPTransport) {
		tr.SetOnPeerDisconnect(func(net.Addr) { disconnected <- struct{}{} })
	})

	conn, err := net.Dial("tcp", tr.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	select {
	case <-disconnected:
		t.Fatal("OnPeerDisconnect fired for a peer that never completed the handshake")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestDisconnectFiresOnce(t *testing.T) {
	connected := make(chan struct{}, 1)
	disconnected := make(chan struct{}, 4)

	tr := startTestTransport(t, TCPTransportOpts{}, func(tr *TCPTransport) {
		tr.SetOnPeer(func(Peer) error {
			connected <- struct{}{}
			return nil
		})
		tr.SetOnPeerDisconnect(func(net.Addr) { disconnected <- struct{}{} })
	})

	conn, err := net.Dial("tcp", tr.Addr().String())
	require.NoError(t, err)

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("peer was never accepted")
	}

	require.NoError(t, conn.Close())

	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("disconnect was never reported")
	}

	select {
	case <-disconnected:
		t.Fatal("disconnect fired more than once")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestNilCallbacksDontPanic(t *testing.T) {
	tr := startTestTransport(t, TCPTransportOpts{}, nil)

	conn, err := net.Dial("tcp", tr.Addr().String())
	require.NoError(t, err)

	// One valid message so the read loop actually runs, then a clean close.
	_, err = conn.Write([]byte{0, 0, 0, 1, 0x01})
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, conn.Close())

	select {
	case msg := <-tr.Consume():
		assert.Equal(t, []byte{0x01}, msg.Payload)
	case <-time.After(2 * time.Second):
		t.Fatal("message was never delivered")
	}

	require.NoError(t, tr.Close())
}

func TestOversizeMessageDisconnects(t *testing.T) {
	disconnected := make(chan struct{}, 1)

	tr := startTestTransport(t, TCPTransportOpts{}, func(tr *TCPTransport) {
		tr.SetOnPeerDisconnect(func(net.Addr) { disconnected <- struct{}{} })
	})

	conn, err := net.Dial("tcp", tr.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], DefaultMaxMessageSize+1)
	_, err = conn.Write(hdr[:])
	require.NoError(t, err)

	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("oversize frame did not drop the peer")
	}
}

func TestCloseReleasesListener(t *testing.T) {
	tr, err := NewTCPTransport(TCPTransportOpts{ListenAddr: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, tr.ListenAndAccept())

	addr := tr.Addr().String()
	require.NoError(t, tr.Close())

	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, ln.Close())
}

func TestTCPPeerConcurrentSendFramesIntact(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
	})

	peer := NewTCPPeer(c1, true, LengthEncoder{}, LengthDecoder{}, 5*time.Second)

	const n = 64
	received := make(chan []byte, n)
	go func() {
		dec := LengthDecoder{}
		for i := 0; i < n; i++ {
			payload, err := dec.Decode(c2)
			if err != nil {
				return
			}
			received <- payload
		}
	}()

	sendErrs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sendErrs <- peer.Send(bytes.Repeat([]byte{byte(i)}, i+1))
		}(i)
	}
	wg.Wait()
	close(sendErrs)

	for err := range sendErrs {
		require.NoError(t, err)
	}

	seen := make(map[int]struct{}, n)
	for i := 0; i < n; i++ {
		select {
		case payload := <-received:
			for _, b := range payload {
				require.Equal(t, byte(len(payload)-1), b)
			}
			seen[len(payload)] = struct{}{}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for framed payloads")
		}
	}
	assert.Len(t, seen, n)
}
