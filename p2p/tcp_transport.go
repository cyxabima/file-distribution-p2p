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
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer{
	return &TCPPeer{
		conn: conn,
		outbound: outbound,
	}
}

// Close() implements Peer Interface 
func (p *TCPPeer) Close() error {
	return  p.conn.Close()
}

type TCPTransportOpts struct{
	ListenAddr string
	HandshakeFunc HandshakeFunc
	Decoder Decoder
	OnPeer func(Peer) error // a function for doing processing on peer
}


type TCPTransport struct {
	TCPTransportOpts
	listner net.Listener
	msgch chan Message

	mu sync.RWMutex
}

func NewTcpTransport(opts TCPTransportOpts) *TCPTransport {

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
		}

		go t.handleConn(conn)
	}
}


func (t* TCPTransport) handleConn(conn net.Conn){
	var err error

	defer func ()  {
		conn.Close()
		// this could be done in there respected if block btw
		if err != nil && err != io.EOF{
		fmt.Printf("Connection closed with error: %v\n", err)
		}else{
		fmt.Printf("Peer is Disconnected Gracefully for %s\n", conn.RemoteAddr())
		}
	}()

	peer := NewTCPPeer(conn, false)

	if err = t.HandshakeFunc(peer); err != nil {
		return
	} 
	
	if err = t.OnPeer(peer) ; err!= nil{
		return
	}

	// Read loop
	msg := Message{}
	for{

		if err := t.Decoder.Decode(conn, &msg); err != nil{
			return
		}
		
		msg.From = conn.RemoteAddr()
		t.msgch <- msg
	} 
}
