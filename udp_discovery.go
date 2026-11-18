package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// 239.0.0.0/8 is the multicast range for local networks.
// Port 9999 is just a convention. Every node must use the same group.
const MulticastGroup = "239.255.255.250:9999"

const (
	// AnnouncementVersion is bumped whenever the packet shape changes. Older
	// packets (version 0) are still accepted so a mixed-version LAN works.
	AnnouncementVersion = 1

	maxPacketSize = 65507
)

// Announcement is one UDP packet: "I am at Addr, and I have these files".
// Only the small FileStub summaries travel here, never the chunk hashes.
type Announcement struct {
	Version int        `json:"version"`
	ID      string     `json:"id"`
	Addr    string     `json:"addr"`
	Files   []FileStub `json:"files"`
}

type UDPDiscovery struct {
	// Config is read once by Start and then snapshotted into the goroutines,
	// so changing these fields afterwards cannot race with them.
	Group        string
	AnnounceAddr string
	Interval     time.Duration
	Iface        *net.Interface

	peers    chan string
	id       string
	registry *Registry

	filesMu sync.RWMutex
	files   []FileStub

	mu       sync.Mutex
	started  bool
	conn     *net.UDPConn // multicast listener
	sendConn *net.UDPConn // announcement sender
	done     chan struct{}
	stopOnce sync.Once

	wg          sync.WaitGroup
	peersClosed chan struct{}
}

func NewUDPDiscovery(announceAddr string, interval time.Duration) *UDPDiscovery {
	if interval <= 0 {
		interval = 3 * time.Second
	}

	return &UDPDiscovery{
		Group:        MulticastGroup,
		AnnounceAddr: announceAddr,
		Interval:     interval,
		peers:        make(chan string, 64),
		id:           newID(),
		registry:     NewRegistry(),
		done:         make(chan struct{}),
		peersClosed:  make(chan struct{}),
	}
}

func (d *UDPDiscovery) SetFiles(files []FileStub) {
	d.filesMu.Lock()
	defer d.filesMu.Unlock()

	d.files = make([]FileStub, len(files))
	copy(d.files, files)
}

func (d *UDPDiscovery) Discover() <-chan string {
	return d.peers
}

func (d *UDPDiscovery) Clusters() []ClusterView {
	return d.registry.GetClusters()
}

func (d *UDPDiscovery) ClusterPeers(infoHash string) []string {
	return d.registry.ClusterPeers(infoHash)
}

// Start joins the multicast group and launches the listener, announcer and
// reaper. It returns any socket error synchronously.
func (d *UDPDiscovery) Start() error {
	// Holding mu through setup means Stop cannot observe a half-started
	// discovery, and two concurrent Start calls cannot both bind the group.
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.started {
		return errors.New("discovery: already started")
	}
	if channelClosed(d.done) {
		return net.ErrClosed
	}

	group := d.Group
	if group == "" {
		group = MulticastGroup
	}
	interval := d.Interval
	if interval <= 0 {
		interval = 3 * time.Second
	}
	iface := d.Iface

	gaddr, err := net.ResolveUDPAddr("udp4", group)
	if err != nil {
		return fmt.Errorf("resolve multicast group: %w", err)
	}

	conn, err := net.ListenMulticastUDP("udp4", iface, gaddr)
	if err != nil {
		return fmt.Errorf("join multicast group: %w", err)
	}
	if err := conn.SetReadBuffer(maxPacketSize); err != nil {
		log.Printf("discovery: set read buffer: %v", err)
	}

	// A separate socket for sending avoids announcing from the listener's
	// ephemeral port. Bind it to the same interface when one was chosen.
	var laddr *net.UDPAddr
	if ip := firstIPv4(iface); ip != nil {
		laddr = &net.UDPAddr{IP: ip}
	}
	sendConn, err := net.DialUDP("udp4", laddr, gaddr)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open announcement socket: %w", err)
	}

	// ":3000" is a valid listen address but a useless announce address: it
	// has no host. Fill in this machine's LAN IP once, here.
	announceAddr := dialableAddr(d.AnnounceAddr, iface)

	d.conn = conn
	d.sendConn = sendConn
	d.started = true

	// Three independent jobs, so three goroutines:
	//   listen   -> receive other nodes' announcements
	//   announce -> send our own announcement on a timer
	//   reap     -> forget peers that went silent
	d.wg.Add(3)
	go d.listen(conn)
	go d.announce(sendConn, interval, announceAddr)
	go d.reap(interval)

	return nil
}

// Stop shuts discovery down cleanly. It never waits, so it is safe to call
// from a callback. Safe to call more than once.
func (d *UDPDiscovery) Stop() {
	d.stopOnce.Do(func() {
		close(d.done)

		d.mu.Lock()
		conn, sendConn := d.conn, d.sendConn
		d.mu.Unlock()

		if conn != nil {
			_ = conn.Close()
		}
		if sendConn != nil {
			_ = sendConn.Close()
		}

		go func() {
			d.wg.Wait()
			// Only listen sends to peers, and it has exited by now.
			close(d.peers)
			close(d.peersClosed)
		}()
	})
}

// Wait blocks until Stop has been called and every goroutine has exited. The
// Discover channel is closed when Wait returns.
func (d *UDPDiscovery) Wait() {
	<-d.peersClosed
}

func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (d *UDPDiscovery) listen(conn *net.UDPConn) {
	defer d.wg.Done()
	defer conn.Close()

	buf := make([]byte, maxPacketSize)

	for {
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			if channelClosed(d.done) {
				return // normal shutdown
			}
			log.Printf("discovery read: %v", err)

			// One bad read must not end discovery forever.
			select {
			case <-d.done:
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		var a Announcement
		if err := json.Unmarshal(buf[:n], &a); err != nil {
			continue
		}
		if a.Version != 0 && a.Version != AnnouncementVersion {
			continue
		}
		if a.ID == d.id || a.Addr == "" {
			continue
		}

		// Trust the packet's port, but not its host: a node that announced
		// 0.0.0.0 or no host is reachable at the address the packet came from.
		addr := withSourceIP(a.Addr, src.IP)
		d.registry.RegisterAnnouncement(a.ID, addr, a.Files)

		select {
		case d.peers <- addr:
		default:
		}
	}
}

func (d *UDPDiscovery) announce(conn *net.UDPConn, interval time.Duration, addr string) {
	defer d.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		d.announceOnce(conn, addr) // send first, so we are discoverable immediately

		select {
		case <-d.done:
			return
		case <-ticker.C:
		}
	}
}

// announceOnce builds the packet from the CURRENT file list and sends it.
func (d *UDPDiscovery) announceOnce(conn *net.UDPConn, addr string) {
	d.filesMu.RLock()
	files := make([]FileStub, len(d.files))
	copy(files, d.files)
	d.filesMu.RUnlock()

	// A very large file set would overflow a UDP datagram. Shrink until it
	// fits rather than letting Write fail with "message too long" forever.
	for {
		payload, err := json.Marshal(Announcement{
			Version: AnnouncementVersion,
			ID:      d.id,
			Addr:    addr,
			Files:   files,
		})
		if err != nil {
			return
		}
		if len(payload) <= maxPacketSize {
			if _, err := conn.Write(payload); err != nil && !channelClosed(d.done) {
				log.Printf("discovery announce: %v", err)
			}
			return
		}

		log.Printf("discovery: announcement too large, truncating from %d files", len(files))
		files = files[:len(files)/2]
	}
}

// reap drops peers that have been silent for longer than 3 announce intervals.
func (d *UDPDiscovery) reap(interval time.Duration) {
	defer d.wg.Done()

	ttl := 3 * interval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-d.done:
			return
		case <-ticker.C:
			d.registry.Sweep(ttl)
		}
	}
}

// dialableAddr replaces an empty or wildcard host with this machine's IPv4
// address. The port is left untouched, so a fixed port still works.
func dialableAddr(addr string, iface *net.Interface) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		return net.JoinHostPort(host, port)
	}

	if ip := firstIPv4(iface); ip != nil {
		return net.JoinHostPort(ip.String(), port)
	}
	if ip := defaultIPv4(); ip != nil {
		return net.JoinHostPort(ip.String(), port)
	}

	log.Printf("discovery: cannot determine a local IP for %q; peers may not be able to dial it", addr)
	return addr
}

// withSourceIP applies the same fix on the receiving side.
func withSourceIP(addr string, ip net.IP) string {
	if ip == nil {
		return addr
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		return addr
	}
	return net.JoinHostPort(ip.String(), port)
}

// defaultIPv4 asks the OS which interface it would use for an outbound UDP
// connection. No packet is actually sent.
func defaultIPv4() net.IP {
	conn, err := net.Dial("udp4", "8.8.8.8:80")
	if err != nil {
		return nil
	}
	defer conn.Close()

	host, _, err := net.SplitHostPort(conn.LocalAddr().String())
	if err != nil {
		return nil
	}
	return net.ParseIP(host).To4()
}

func firstIPv4(iface *net.Interface) net.IP {
	if iface == nil {
		return nil
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return nil
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok {
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				return ip4
			}
		}
	}
	return nil
}

func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
