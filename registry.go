package main

// This file is the "who has what" memory of the discovery layer.

import (
	"sort"
	"sync"
	"time"
)

// Announcements arrive every few seconds from every node on the LAN. These
// limits keep a hostile or buggy neighbour from growing the table forever.
const (
	MaxClusters     = 10_000
	MaxPeersPerFile = 1_000
)

type FileStub struct {
	InfoHash string `json:"info_hash"`
	FileName string `json:"file_name"`
	FileSize int64  `json:"file_size"`
}

type Cluster struct {
	InfoHash string
	FileName string
	FileSize int64
	Peers    map[string]time.Time
}

type ClusterView struct {
	InfoHash string
	FileName string
	FileSize int64
	Peers    []string
}

type Registry struct {
	mu         sync.RWMutex
	clusters   map[string]*Cluster
	now        func() time.Time
	identities map[string]string
}

func NewRegistry() *Registry {
	return &Registry{
		clusters:   make(map[string]*Cluster),
		identities: make(map[string]string),
		now:        time.Now,
	}
}

func (r *Registry) RegisterAnnouncement(id, addr string, files []FileStub) {
	if addr == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Same node, new address (DHCP or reconnect): remove the old address
	// first, otherwise one node appears twice in every cluster until the TTL
	// sweep finally forgets the stale entry.
	if old, ok := r.identities[id]; ok && old != addr {
		r.removeAddrLocked(old)
		delete(r.identities, id)
	}
	r.identities[id] = addr

	want := make(map[string]struct{}, len(files))
	for _, f := range files {
		if f.InfoHash != "" {
			want[f.InfoHash] = struct{}{}
		}
	}
	for infoHash, c := range r.clusters {
		if _, ok := want[infoHash]; ok {
			continue
		}
		delete(c.Peers, addr)
		if len(c.Peers) == 0 {
			delete(r.clusters, infoHash)
		}
	}

	now := r.now()
	for _, f := range files {
		if f.InfoHash == "" {
			continue
		}

		c, ok := r.clusters[f.InfoHash]
		if !ok {
			if len(r.clusters) >= MaxClusters {
				break
			}
			c = &Cluster{
				InfoHash: f.InfoHash,
				Peers:    make(map[string]time.Time),
			}
			r.clusters[f.InfoHash] = c
		}

		if _, exists := c.Peers[addr]; !exists && len(c.Peers) >= MaxPeersPerFile {
			continue
		}

		// An empty name is missing information, not a correction. Only let a
		// named stub overwrite the metadata for the whole cluster.
		if f.FileName != "" {
			c.FileName = f.FileName
			c.FileSize = f.FileSize
		}
		c.Peers[addr] = now
	}
}

// RegisterBlueprint is kept for callers without a node ID; it treats the
// address as the identity.
func (r *Registry) RegisterBlueprint(addr string, files []FileStub) {
	r.RegisterAnnouncement(addr, addr, files)
}

// UnregisterBlueprint removes one peer from one file. If that was the last
// peer, the whole cluster is deleted.
func (r *Registry) UnregisterBlueprint(addr, infoHash string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	c, ok := r.clusters[infoHash]
	if !ok {
		return
	}

	delete(c.Peers, addr)
	if len(c.Peers) == 0 {
		delete(r.clusters, infoHash)
	}
	if !r.addrPresentLocked(addr) {
		r.deleteIdentityForAddrLocked(addr)
	}
}

func (r *Registry) UnregisterPeer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removeAddrLocked(addr)
	r.deleteIdentityForAddrLocked(addr)
}

func (r *Registry) Sweep(ttl time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()

	for infoHash, c := range r.clusters {
		for addr, lastSeen := range c.Peers {
			if now.Sub(lastSeen) > ttl {
				delete(c.Peers, addr)
				r.deleteIdentityForAddrLocked(addr)
			}
		}
		if len(c.Peers) == 0 {
			delete(r.clusters, infoHash)
		}
	}
}

func (r *Registry) GetClusters() []ClusterView {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]ClusterView, 0, len(r.clusters))

	for _, c := range r.clusters {
		v := ClusterView{
			InfoHash: c.InfoHash,
			FileName: c.FileName,
			FileSize: c.FileSize,
			Peers:    make([]string, 0, len(c.Peers)),
		}
		for addr := range c.Peers {
			v.Peers = append(v.Peers, addr)
		}
		sort.Strings(v.Peers) // map order is random; make it readable
		out = append(out, v)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].InfoHash < out[j].InfoHash })
	return out
}

func (r *Registry) ClusterPeers(infoHash string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	c, ok := r.clusters[infoHash]
	if !ok {
		return []string{}
	}

	peers := make([]string, 0, len(c.Peers))
	for addr := range c.Peers {
		peers = append(peers, addr)
	}
	sort.Strings(peers)
	return peers
}

// removeAddrLocked removes addr from every cluster and deletes empty ones.
// The caller must hold mu.
func (r *Registry) removeAddrLocked(addr string) {
	for infoHash, c := range r.clusters {
		delete(c.Peers, addr)
		if len(c.Peers) == 0 {
			delete(r.clusters, infoHash)
		}
	}
}

// deleteIdentityForAddrLocked drops the ID -> addr entries for one address.
// The caller must hold mu.
func (r *Registry) deleteIdentityForAddrLocked(addr string) {
	for id, a := range r.identities {
		if a == addr {
			delete(r.identities, id)
		}
	}
}

// addrPresentLocked reports whether addr still appears in any cluster.
// The caller must hold mu.
func (r *Registry) addrPresentLocked(addr string) bool {
	for _, c := range r.clusters {
		if _, ok := c.Peers[addr]; ok {
			return true
		}
	}
	return false
}

// NOTE: LOCK Suffix is just a convention that these function should be called after lock
