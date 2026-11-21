package p2p

import (
	"errors"
	"io"
	"net"
	"runtime/debug"
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
	writeMu      sync.Mutex
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

	// Logger receives transport diagnostics. Defaults to log.Default().
	Logger Logger
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
	o.Logger = loggerOr(o.Logger)

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

	// loggerMu guards Logger because SetLogger may be called after Start.
	loggerMu sync.RWMutex

	done      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// logf snapshots the current logger under a lock. Taking the function form
// keeps call from holding a possibly-changing interface value.
func (t *TCPTransport) logf(format string, args ...any) {
	t.loggerMu.RLock()
	l := t.Logger
	t.loggerMu.RUnlock()
	l.Printf(format, args...)
}

// SetLogger redirects transport diagnostics. It is safe to call after Start.
func (t *TCPTransport) SetLogger(l Logger) {
	t.loggerMu.Lock()
	t.Logger = loggerOr(l)
	t.loggerMu.Unlock()
}

// call runs fn and converts a panic into a logged error, so a buggy
// application callback cannot take down the whole process. It reports whether
// fn panicked. logf is supplied by the caller so the message reaches that
// component's logger rather than the process-wide stderr.
func call(logf func(string, ...any), name string, fn func()) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			logf("p2p: panic in %s: %v\n%s", name, r, debug.Stack())
		}
	}()

	fn()
	return false
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
		stopped:          make(chan struct{}),
	}, nil
}

// Consume returns a read-only channel of messages received from peers. The
// channel is closed when the transport is closed.
func (t *TCPTransport) Consume() <-chan Message {
	return t.msgch
}

// SetOnPeer installs the callback run for every accepted peer. It is safe to
// call at any time; a connection already in flight keeps the callbacks it
// snapshotted when it started.
func (t *TCPTransport) SetOnPeer(fn func(Peer) error) {
	if fn == nil {
		fn = func(Peer) error { return nil }
	}

	t.mu.Lock()
	t.onPeer = fn
	t.mu.Unlock()
}

func (t *TCPTransport) SetOnPeerDisconnect(fn func(net.Addr)) {
	if fn == nil {
		fn = func(net.Addr) {}
	}

	t.mu.Lock()
	t.onPeerDisconnect = fn
	t.mu.Unlock()
}

// callbacks returns a consistent pair for one connection.
func (t *TCPTransport) callbacks() (func(Peer) error, func(net.Addr)) {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.onPeer, t.onPeerDisconnect
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

// Close stops accepting, closes every live connection and signals the
// background work to stop. It never waits, so it is safe to call from any
// goroutine — including an OnPeer or OnPeerDisconnect callback. It is
// idempotent.
//
// Use Wait to block until all handlers have exited and Consume has been
// closed.
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

		// Wait off the caller's goroutine: a callback may be the caller, and
		// that goroutine is itself counted in wg.
		go func() {
			t.wg.Wait()
			// Safe to close msgch: every producer has exited by now.
			close(t.msgch)
			close(t.stopped)
		}()
	})

	return nil
}

// Wait blocks until Close has been called, every handler has exited and the
// Consume channel is closed. Calling Wait without Close blocks forever.
func (t *TCPTransport) Wait() {
	<-t.stopped
}

func (t *TCPTransport) startAcceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			t.logf("p2p: accept error: %v", err)
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

	// Snapshot the callbacks once so one connection sees a consistent pair
	// even if the application swaps them while it is running.
	onPeer, onDisconnect := t.callbacks()

	// accepted is only true once the application has seen the peer, so a
	// failed handshake or a rejected OnPeer never produces a disconnect event.
	accepted := false

	defer func() {
		conn.Close()
		t.untrackConn(conn)

		if accepted {
			call(t.logf, "OnPeerDisconnect", func() { onDisconnect(remoteAddr) })
		}

		switch {
		case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
			// Do nothing clean shutdown
		case err != nil:
			t.logf("p2p: connection %s closed: %v", remoteAddr, err)
		}
	}()

	if err = conn.SetDeadline(time.Now().Add(t.HandshakeTimeout)); err != nil {
		return
	}

	peer := NewTCPPeer(conn, outbound, t.Encoder, t.Decoder, t.WriteTimeout)

	if err = t.HandshakeFunc(peer); err != nil {
		return
	}

	var perr error
	if call(t.logf, "OnPeer", func() { perr = onPeer(peer) }) {
		err = errors.New("OnPeer panicked")
		return
	}
	if perr != nil {
		err = perr
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
