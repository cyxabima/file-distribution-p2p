package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultListenAddr = ":3000"
	defaultChunkSize  = 256 * 1024

	redialInterval = 2 * time.Second
	retryCooldown  = 10 * time.Second
)

// addrList lets --peer be passed more than once.
type addrList []string

func (a *addrList) String() string { return strings.Join(*a, ",") }

func (a *addrList) Set(v string) error {
	if v = strings.TrimSpace(v); v != "" {
		*a = append(*a, v)
	}
	return nil
}

// dialTracker stops us from dialing the same address repeatedly while a
// previous attempt is still in progress or just failed.
type dialTracker struct {
	mu        sync.Mutex
	lastTry   map[string]time.Time
	connected map[string]bool
}

func newDialTracker() *dialTracker {
	return &dialTracker{
		lastTry:   make(map[string]time.Time),
		connected: make(map[string]bool),
	}
}

func (t *dialTracker) try(addr string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.connected[addr] {
		return false
	}
	if last, ok := t.lastTry[addr]; ok && time.Since(last) < retryCooldown {
		return false
	}
	t.lastTry[addr] = time.Now()
	return true
}

// tryManual ignores the discovery cooldown: a manually supplied peer is
// retried every redialInterval until it comes up.
func (t *dialTracker) tryManual(addr string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.connected[addr] {
		return false
	}
	t.lastTry[addr] = time.Now()
	return true
}

func (t *dialTracker) markConnected(addr string) {
	t.mu.Lock()
	t.connected[addr] = true
	t.mu.Unlock()
}

func main() {
	seedPath := flag.String("seed", "", "path to an existing file to seed")
	fetch := flag.Bool("fetch", false, "download a file from peers")
	listen := flag.String("listen", defaultListenAddr, "TCP listen address (host:port)")
	storage := flag.String("storage", defaultStorageDir, "directory for blueprints and downloads")
	chunkSize := flag.Uint("chunk-size", defaultChunkSize, "chunk size in bytes")
	useDiscovery := flag.Bool("discovery", true, "enable UDP LAN discovery")
	var peers addrList
	flag.Var(&peers, "peer", "peer address host:port (repeatable)")
	flag.Parse()

	if *seedPath == "" && !*fetch {
		log.Fatal("choose --seed <file> or --fetch")
	}
	if *seedPath != "" && *fetch {
		log.Fatal("--seed and --fetch are mutually exclusive")
	}
	if *chunkSize == 0 || *chunkSize > uint(MaxChunkSize) {
		log.Fatalf("chunk size must be between 1 and %d bytes", MaxChunkSize)
	}

	// One logger for the whole process; a TUI can replace this with a discard
	// or file logger before doing anything else.
	logger := loggerOr(nil)

	app, err := NewFileShareApp(*listen, *storage)
	if err != nil {
		log.Fatal(err)
	}
	app.SetLogger(logger)

	if *seedPath != "" {
		b, err := app.LoadSeedFile(*seedPath, uint32(*chunkSize))
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("seeding %s (%d bytes, %d chunks), info hash %s",
			b.FileName, b.FileSize, len(b.Hashes), b.InfoHash)
	} else {
		log.Printf("fetching; waiting for a blueprint from a peer")
	}

	app.Start()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tracker := newDialTracker()
	for _, addr := range peers {
		go connectLoop(ctx, app, tracker, addr)
	}

	var discovery *UDPDiscovery
	if *useDiscovery {
		announceAddr := dialableAddr(*listen, nil)
		discovery = NewUDPDiscovery(announceAddr, 3*time.Second)
		discovery.SetLogger(logger)

		if *seedPath != "" {
			if b := app.session().blueprint; b != nil {
				discovery.SetFiles([]FileStub{b.Stub()})
			}
		}

		if err := discovery.Start(); err != nil {
			log.Printf("UDP discovery unavailable: %v (use --peer to connect manually)", err)
			discovery = nil
		} else {
			log.Printf("discovery listening on %s, announcing %s", MulticastGroup, announceAddr)
			go discoverLoop(ctx, app, discovery, tracker, announceAddr)
		}
	}

	if *fetch {
		go waitForDownload(ctx, app, discovery)
	}

	<-ctx.Done()
	log.Println("shutting down")

	if discovery != nil {
		discovery.Stop()
		discovery.Wait()
	}
	if err := app.Stop(); err != nil {
		log.Printf("stop app: %v", err)
	}
	app.Wait()
}

// connectLoop keeps trying one manually supplied peer until it connects or
// the process is interrupted. A failed first attempt (seeder not up yet) is
// normal, so retrying is friendlier than making the user restart us.
func connectLoop(ctx context.Context, app *FileShareApp, tracker *dialTracker, addr string) {
	for {
		if tracker.tryManual(addr) {
			if err := app.swarm.Dial(addr); err != nil {
				log.Printf("dial %s: %v (retrying)", addr, err)
			} else {
				tracker.markConnected(addr)
				log.Printf("connected to peer %s", addr)
				return
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(redialInterval):
		}
	}
}

// discoverLoop turns announced addresses into outbound connections.
func discoverLoop(ctx context.Context, app *FileShareApp, d *UDPDiscovery, tracker *dialTracker, selfAddr string) {
	for {
		select {
		case addr, ok := <-d.Discover():
			if !ok {
				return
			}
			if addr == "" || addr == selfAddr || !tracker.try(addr) {
				continue
			}

			go func(a string) {
				if err := app.swarm.Dial(a); err != nil {
					log.Printf("discovered peer %s: %v", a, err)
					return
				}
				tracker.markConnected(a)
				log.Printf("connected to discovered peer %s", a)
			}(addr)

		case <-ctx.Done():
			return
		}
	}
}

// waitForDownload reports completion and turns this leecher into a seeder:
// it already has every chunk, so announcing the stub lets other peers fetch
// from us too. The download itself is handled by the request engine and
// handlePiece; this function only watches progress.
func waitForDownload(ctx context.Context, app *FileShareApp, discovery *UDPDiscovery) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sess := app.session()
			if sess.state == nil || sess.verifier == nil || sess.blueprint == nil {
				continue
			}

			total := uint32(len(sess.verifier.expectedHashes))
			if total == 0 {
				continue
			}

			complete := true
			for i := uint32(0); i < total; i++ {
				if !sess.state.MyBitfield.Has(i) {
					complete = false
					break
				}
			}
			if complete {
				out := filepath.Join(app.storageDir, "downloads", sess.blueprint.FileName)
				log.Printf("download complete: %s -> %s", sess.blueprint.FileName, out)

				if discovery != nil {
					discovery.SetFiles([]FileStub{sess.blueprint.Stub()})
					log.Printf("now seeding %s to the LAN", sess.blueprint.InfoHash)
				}
				return
			}

		case <-ctx.Done():
			return
		}
	}
}
