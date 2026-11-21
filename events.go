package main

// EventType identifies what happened. Values are kept as strings so a UI can
// switch on them (and log them) without importing any app internals.
type EventType string

const (
	PeerConnected    EventType = "peer_connected"
	PeerDisconnected EventType = "peer_disconnected"
	BlueprintLoaded  EventType = "blueprint_loaded"
	ChunkDownloaded  EventType = "chunk_downloaded"
	PieceServed      EventType = "piece_served"
	DownloadComplete EventType = "download_complete"
	Error            EventType = "error"
)

// Event is one observable change in the app. Fields not relevant to an event
// type are left at their zero value, so consumers only read what they need.
type Event struct {
	Type        EventType
	InfoHash    string
	FileName    string
	PeerAddr    string
	ChunkIndex  uint32
	ChunksDone  uint32
	ChunksTotal uint32
	Err         error
}

// fileInfo fills the file fields shared by most events.
func fileInfo(sess fileSession) (infoHash, fileName string) {
	if sess.blueprint == nil {
		return "", ""
	}
	return sess.blueprint.InfoHash, sess.blueprint.FileName
}
