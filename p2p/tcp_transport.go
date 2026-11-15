package p2p

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// Transport-level defaults and the generous on purpose as slow link should survive
// but a stalled peer should not.
const (
	DefaultHandshakeTimeout = 5 * time.Second
	DefaultIdleTimeout      = 60 * time.Second
	DefaultWriteTimeout     = 10 * time.Second
	DefaultDialTimeout      = 10 * time.Second

	defaultMsgChanBuffer = 64
)

// ErrCodecPair is returned when only one half of the wire format is supplied.
var ErrCodecPair = errors.New("p2p: Encoder and Decoder must be supplied together")

// TCPPeer represents the remote node over an established TCP connection.
type TCPPeer struct {
	conn net.Conn
	// outbound is true when we dialed the connection, false when we accepted it.
	outbound bool

	encoder Encoder
	decoder Decoder

	// writeMu prevents interleaved frames if two goroutines Send at once.
	writeMu sync.Mutex
	writeTimeout time.Duration
}

func NewTCPPeer(conn net.Conn, outbound bool, encoder Encoder, decoder Decoder, writeTimeout time.Duration) *TCPPeer {
	return &TCPPeer{
		conn:         conn,
		outbound:     outbound,
		encoder:      encoder,
		decoder:      decoder,
		writeTimeout: writeTimeout,
	}
}

func (p *TCPPeer) Close() error {
	return p.conn.Close()
}

func (p *TCPPeer) RemoteAddr() net.Addr {
	return p.conn.RemoteAddr()
}

// Send writes one whole payload. A nil payload is a keep-alive frame.
//
// If Send returns an error the stream may be mid-frame, so the peer must be
// closed rather than reused.
func (p *TCPPeer) Send(payload []byte) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	if p.writeTimeout > 0 {
		if err := p.conn.SetWriteDeadline(time.Now().Add(p.writeTimeout)); err != nil {
			return err
		}
	}

	return p.encoder.Encode(p.conn, payload)
}

func (p *TCPPeer) Receive() ([]byte, error) {
	return p.decoder.Decode(p.conn)
}

type TCPTransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakeFunc

	Encoder Encoder
	Decoder Decoder

	HandshakeTimeout time.Duration
	IdleTimeout      time.Duration
	WriteTimeout     time.Duration
	DialTimeout      time.Duration
}

func (o TCPTransportOpts) withDefaults() (TCPTransportOpts, error) {
	switch {
	case o.Encoder == nil && o.Decoder == nil:
		o.Encoder = LengthEncoder{}
		o.Decoder = LengthDecoder{}
	case o.Encoder == nil || o.Decoder == nil:
		return o, ErrCodecPair
	}

	if o.HandshakeFunc == nil {
		o.HandshakeFunc = NOPHandshakeFunc
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = DefaultIdleTimeout
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = DefaultWriteTimeout
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = DefaultDialTimeout
	}

	return o, nil
}

type TCPTransport struct {
	TCPTransportOpts

	msgch chan Message

	// Callbacks are unexported and default to no-ops so a peer can never panic
	// the accept goroutine. Set them through the setters, before listening.
	onPeer           func(Peer) error
	onPeerDisconnect func(net.Addr)

	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	closed   bool

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func NewTCPTransport(opts TCPTransportOpts) (*TCPTransport, error) {
	opts, err := opts.withDefaults()
	if err != nil {
		return nil, err
	}

	return &TCPTransport{
		TCPTransportOpts: opts,
		msgch:            make(chan Message, defaultMsgChanBuffer),
		onPeer:           func(Peer) error { return nil },
		onPeerDisconnect: func(net.Addr) {},
		conns:            make(map[net.Conn]struct{}),
		done:             make(chan struct{}),
	}, nil
}

// Consume returns a read-only channel of messages received from peers. The
// channel is closed when the transport is closed.
func (t *TCPTransport) Consume() <-chan Message {
	return t.msgch
}

func (t *TCPTransport) SetOnPeer(fn func(Peer) error) {
	if fn == nil {
		fn = func(Peer) error { return nil }
	}
	t.onPeer = fn
}

func (t *TCPTransport) SetOnPeerDisconnect(fn func(net.Addr)) {
	if fn == nil {
		fn = func(net.Addr) {}
	}
	t.onPeerDisconnect = fn
}

// Addr returns the address the transport is listening on, or nil before
// ListenAndAccept. Useful with a ":0" listen address.
func (t *TCPTransport) Addr() net.Addr {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.listener == nil {
		return nil
	}
	return t.listener.Addr()
}

func (t *TCPTransport) ListenAndAccept() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return net.ErrClosed
	}
	if t.listener != nil {
		t.mu.Unlock()
		return errors.New("p2p: transport is already listening")
	}

	ln, err := net.Listen("tcp", t.ListenAddr)
	if err != nil {
		t.mu.Unlock()
		return err
	}

	t.listener = ln
	t.wg.Add(1)
	t.mu.Unlock()

	go func() {
		defer t.wg.Done()
		t.startAcceptLoop(ln)
	}()

	return nil
}

func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, t.DialTimeout)
	if err != nil {
		return err
	}

	// The handshake runs in the handler goroutine, so a successful Dial only
	// means the TCP connection was established, not that the peer joined.
	return t.startConn(conn, true)
}

// Close stops accepting, closes every live connection, waits for the handler
// goroutines to exit and then closes the Consume channel.
func (t *TCPTransport) Close() error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		ln := t.listener
		conns := make([]net.Conn, 0, len(t.conns))
		for c := range t.conns {
			conns = append(conns, c)
		}
		t.mu.Unlock()

		close(t.done)
		if ln != nil {
			_ = ln.Close()
		}
		for _, c := range conns {
			_ = c.Close()
		}

		// Safe to close msgch: every producer (accept loop and connection
		// handlers) is registered in wg and has exited by now.
		t.wg.Wait()
		close(t.msgch)
	})

	return nil
}

func (t *TCPTransport) startAcceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("p2p: accept error: %v", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if err := t.startConn(conn, false); err != nil {
			return
		}
	}
}

// startConn registers a live connection and launches its handler. It refuses
// new connections once the transport is closing.
func (t *TCPTransport) startConn(conn net.Conn, outbound bool) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = conn.Close()
		return net.ErrClosed
	}

	t.conns[conn] = struct{}{}
	t.wg.Add(1)
	t.mu.Unlock()

	go func() {
		defer t.wg.Done()
		t.handleConn(conn, outbound)
	}()

	return nil
}

func (t *TCPTransport) untrackConn(conn net.Conn) {
	t.mu.Lock()
	delete(t.conns, conn)
	t.mu.Unlock()
}

func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	var err error
	remoteAddr := conn.RemoteAddr()

	// accepted is only true once the application has seen the peer, so a
	// failed handshake or a rejected OnPeer never produces a disconnect event.
	accepted := false

	defer func() {
		conn.Close()
		t.untrackConn(conn)

		if accepted {
			t.onPeerDisconnect(remoteAddr)
		}

		switch {
		case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
			// TODO: clean shutdown
		case err != nil:
			log.Printf("p2p: connection %s closed: %v", remoteAddr, err)
		}
	}()

	if err = conn.SetDeadline(time.Now().Add(t.HandshakeTimeout)); err != nil {
		return
	}

	peer := NewTCPPeer(conn, outbound, t.Encoder, t.Decoder, t.WriteTimeout)

	if err = t.HandshakeFunc(peer); err != nil {
		return
	}

	if err = t.onPeer(peer); err != nil {
		return
	}
	accepted = true

	// Clear the handshake deadline; the read loop sets its own per message
	if err = conn.SetDeadline(time.Time{}); err != nil {
		return
	}

	for {
		if err = conn.SetReadDeadline(time.Now().Add(t.IdleTimeout)); err != nil {
			return
		}

		payload, rerr := peer.Receive()
		switch {
		case errors.Is(rerr, ErrKeepAlive):
			err = nil
			continue
		case rerr != nil:
			err = rerr
			return
		}

		msg := Message{
			From:    remoteAddr,
			Payload: payload,
			Peer:    peer,
		}

		select {
		case t.msgch <- msg:
		case <-t.done:
			err = net.ErrClosed
			return
		}
	}
}
