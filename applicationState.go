package main

import (
	"net"
	"sync"
	"time"
)

type ApplicationState struct {
	MyBitfiled *Bitfield
	mu         sync.RWMutex

	peerBitfields map[net.Addr]*Bitfield
	// uint32 here is for chunck index
	inFlight map[uint32]PendingRequest
}

func NewApplicationState(totalChunks uint32) *ApplicationState {
	return &ApplicationState{
		MyBitfiled:    NewBitfield(totalChunks),
		peerBitfields: make(map[net.Addr]*Bitfield),
		inFlight:      make(map[uint32]PendingRequest),
	}
}

func (s *ApplicationState) SetPeerBitfields(addr net.Addr, bitfield *Bitfield) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.peerBitfields[addr] = bitfield
}

// Mark a chunk as requested so we donot ask for it twice
func (s *ApplicationState) MarkInFlight(index uint32, addr net.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight[index] = PendingRequest{
		PeerAddr:    addr,
		RequestedAt: time.Now(),
	}
}

func (s *ApplicationState) SweepTimeouts(timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for index, req := range s.inFlight {
		if now.Sub(req.RequestedAt) > timeout {
			delete(s.inFlight, index)
		}
	}
}

func (s *ApplicationState) FindNextRequest(totalChunks uint32) (uint32, net.Addr, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var bestChunk uint32
	var bestPeer net.Addr
	found := false

	// Start with the maximum possible integer
	minAvailability := int(^uint(0) >> 1)

	for i := uint32(0); i <= totalChunks; i++ {

		// skip if already have chunk
		if s.MyBitfiled.Has(i) {
			continue
		}

		// skip if already inflight
		if _, ok := s.inFlight[i]; ok {
			continue
		}

		// count how many peer have this specific chunk
		availability := 0
		var availablePeer net.Addr

		for addr, peerBf := range s.peerBitfields {
			if peerBf.Has(i) {
				availability++
				availablePeer = addr
			}
		}

		if availability > 0 && availability < minAvailability {
			minAvailability = availability
			bestChunk = i
			bestPeer = availablePeer
			found = true

			// cuz we are finding rearest and if availability is 1 it is rearest
			if availability == 1 {
				break
			}
		}
	}

	return bestChunk, bestPeer, found

}
