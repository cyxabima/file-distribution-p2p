package main

import (
	"fmt"
	"log"

	"github.com/cyxabima/minimal-p2p-library-in-go/p2p"
)

func OnPeer(peer p2p.Peer) error {
	fmt.Println("Doing Some logic on Peer outside p2p package")
	return nil
}

func main() {
	tcpOpts := p2p.TCPTransportOpts{
		ListenAddr:    ":3000",
		HandshakeFunc: p2p.NOPHandshakeFunc,
	}

	tr := p2p.NewTcpTransport(tcpOpts)

	go func() {
		for {
			msg := <-tr.Consume()
			fmt.Printf("%+v\n", msg)
		}
	}()

	err := tr.ListenAndAccept()
	if err != nil {
		log.Fatal(err)
	}

	// to prevetnt out program eneding
	select {}

}
