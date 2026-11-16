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

	tr, err := p2p.NewTCPTransport(tcpOpts)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for msg := range tr.Consume() {
			fmt.Printf("%+v\n", msg)
		}
	}()

	if err := tr.ListenAndAccept(); err != nil {
		log.Fatal(err)
	}

	// to prevetnt out program eneding
	select {}

}
