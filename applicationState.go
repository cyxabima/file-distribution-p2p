package main

import (
	"net"
	"sync"
	"time"
)

type ApplicationState struct{
	MyBitfiled *Bitfield
	mu sync.RWMutex

	peerBitfields map[net.Addr]*Bitfield
	inFlight map[uint32]PendingRequest
}

func NewApplicationState(totalChunks uint32) *ApplicationState{
	return &ApplicationState{
		MyBitfiled: NewBitfield(totalChunks),
		peerBitfields: make(map[net.Addr]*Bitfield),
		inFlight:      make(map[uint32]PendingRequest),
	}
}

func (s *ApplicationState) SetPeerBitfields(addr net.Addr, bitfield *Bitfield){
	s.mu.Lock()
	defer s.mu.Unlock()

	s.peerBitfields[addr] = bitfield
}

// Mark a chunk as requested so we donot ask for it twice
func (s *ApplicationState) MakeInFlight(index uint32, addr net.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight[index] = PendingRequest{
	PeerAddr: addr,
	RequestedAt: time.Now(),
	}
}

