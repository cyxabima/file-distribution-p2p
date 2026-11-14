package p2p

import (
	"fmt"
	"io"
	"net"
	"sync"
)

// TCPPeer represents the remote node over a TCP established connection
type TCPPeer struct{
	conn net.Conn
// if we dial and retrive a conn => outbound == true
// of we accpect and retrive a conn => outbound == false
	outbound bool
	encoder Encoder
	writeMu  sync.Mutex   // Prevents interleaved frames if two goroutines Send() at once
}

func NewTCPPeer(conn net.Conn, outbound bool, encoder Encoder) *TCPPeer{
	return &TCPPeer{
		conn: conn,
		outbound: outbound,
		encoder: encoder,
	}
}

func (p *TCPPeer) Close() error {
	return  p.conn.Close()
}

func (p *TCPPeer) RemoteAddr() net.Addr{
	return p.conn.RemoteAddr()
}

func (p *TCPPeer) Send(payload []byte) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	return p.encoder.Encode(p.conn, payload)
}

func (p *TCPPeer) Read(b []byte) (int, error) {
	return p.conn.Read(b)
}

type TCPTransportOpts struct{
	ListenAddr string
	HandshakeFunc HandshakeFunc
	Decoder Decoder
	Encoder Encoder
}


type TCPTransport struct {
	TCPTransportOpts
	listner net.Listener
	msgch chan Message
	OnPeer func(Peer) error // a function for doing processing on peer
}

func NewTcpTransport(opts TCPTransportOpts) *TCPTransport {
	if opts.Decoder == nil {
		opts.Decoder = DefaultDecoder{}
	}
	
	// Fallback to DefaultEncoder if application didn't provide one
	if opts.Encoder == nil {
		opts.Encoder = DefaultEncoder{}
	}

	return &TCPTransport{
		TCPTransportOpts: opts,
		msgch: make(chan Message),
	}
}

// It returns a read only channel for reading the Incoming message from other peers
func (t *TCPTransport) Consume() <- chan Message{
	return t.msgch
}


func (t *TCPTransport) ListenAndAccept() (error) {
	var err error
	t.listner , err = net.Listen("tcp", t.ListenAddr)
	if err != nil{
		return  err
	}
	go t.startAcceptLoop()
	return  nil
}


func (t *TCPTransport) startAcceptLoop(){
	for {
		conn, err := t.listner.Accept()
		if err != nil {
			fmt.Printf("Tcp accpect error: %s\n", err)
			continue
		}

		go t.handleConn(conn,false)
	}
}

func (t *TCPTransport) Dial(adrr string) error{
	conn , err := net.Dial("tcp",adrr)
	if err != nil {
		return err
	}
	go  t.handleConn(conn, true)
	return nil
}

func (t* TCPTransport) handleConn(conn net.Conn, outbound bool){
	var err error

	defer func ()  {
		conn.Close()
		// this could be done in there respected if block btw
		if err != nil && err != io.EOF {
			fmt.Printf("Connection closed with error: %v\n", err)
		}else{
			fmt.Printf("Peer is Disconnected Gracefully for %s\n", conn.RemoteAddr())
		}
	}()

	peer := NewTCPPeer(conn, outbound, t.Encoder)

	if err = t.HandshakeFunc(peer); err != nil {
		return
	}
	
	if err = t.OnPeer(peer) ; err != nil{
		return
	}

	// Read loop
	msg := Message{}
	for{

		if err = t.Decoder.Decode(conn, &msg); err != nil{
			return
		}
		
		msg.From = conn.RemoteAddr()
		msg.Peer = peer
		t.msgch <- msg
	}
}

func (t *TCPTransport) SetOnPeer(fn func(Peer) error) {
	t.OnPeer = fn
}
