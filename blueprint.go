package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cyxabima/minimal-p2p-library-in-go/p2p"
)

// MaxChunkSize leaves room in a MsgPiece for the type byte and 4-byte index.
// It must stay below the transport's frame limit or no chunk can ever be sent.
const MaxChunkSize = p2p.DefaultMaxMessageSize - 5

// Blueprint is the full description of one shared file. UDP announcements
// carry only a FileStub; the chunk hashes travel here, over TCP.
type Blueprint struct {
	InfoHash  string   `json:"info_hash"`
	FileName  string   `json:"file_name"`
	FileSize  int64    `json:"file_size"`
	ChunkSize uint32   `json:"chunk_size"`
	Hashes    [][]byte `json:"hashes"` // JSON encodes each []byte as base64
}

func (b *Blueprint) Stub() FileStub {
	return FileStub{
		InfoHash: b.InfoHash,
		FileName: b.FileName,
		FileSize: b.FileSize,
	}
}

// ComputeInfoHash derives a stable ID from everything that affects the
// download. Both seeder and leecher run this, so they must agree byte for byte.
func ComputeInfoHash(name string, size int64, chunkSize uint32, hashes [][]byte) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%d\x00%d\x00", name, size, chunkSize)
	for _, hash := range hashes {
		h.Write(hash)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// BuildBlueprint hashes a file and collects the metadata both peers need.
func BuildBlueprint(path string, chunkSize uint32) (*Blueprint, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is a directory", path)
	}

	hashes, err := GenerateHash(path, chunkSize)
	if err != nil {
		return nil, err
	}

	b := &Blueprint{
		InfoHash:  ComputeInfoHash(info.Name(), info.Size(), chunkSize, hashes),
		FileName:  info.Name(),
		FileSize:  info.Size(),
		ChunkSize: chunkSize,
		Hashes:    hashes,
	}
	if err := ValidateBlueprint(b); err != nil {
		return nil, err
	}
	return b, nil
}

// ValidateBlueprint rejects a malformed or tampered blueprint before any of
// its fields are used to open files or index hashes.
func ValidateBlueprint(b *Blueprint) error {
	if b == nil {
		return fmt.Errorf("nil blueprint")
	}
	if b.InfoHash == "" || b.FileName == "" || b.FileSize < 0 || b.ChunkSize == 0 {
		return fmt.Errorf("incomplete blueprint")
	}
	if b.ChunkSize > MaxChunkSize {
		return fmt.Errorf("chunk size %d exceeds transport limit %d", b.ChunkSize, MaxChunkSize)
	}
	if _, err := safeFileName(b.FileName); err != nil {
		return err
	}

	want := (b.FileSize + int64(b.ChunkSize) - 1) / int64(b.ChunkSize)
	if int64(len(b.Hashes)) != want {
		return fmt.Errorf("blueprint has %d hashes, need %d", len(b.Hashes), want)
	}
	for i, hash := range b.Hashes {
		if len(hash) != sha256.Size {
			return fmt.Errorf("hash %d is %d bytes, need %d", i, len(hash), sha256.Size)
		}
	}
	if got := ComputeInfoHash(b.FileName, b.FileSize, b.ChunkSize, b.Hashes); got != b.InfoHash {
		return fmt.Errorf("info hash mismatch: got %s, computed %s", b.InfoHash, got)
	}
	return nil
}

func safeFileName(name string) (string, error) {
	if name == "" || filepath.Base(name) != name || name == "." || name == ".." ||
		strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("invalid file name %q", name)
	}
	return name, nil
}

// BlueprintPath keeps a peer-supplied info hash from escaping the blueprint
// directory (e.g. "../../etc/passwd").
func BlueprintPath(dir, infoHash string) (string, error) {
	if len(infoHash) != sha256.Size*2 {
		return "", fmt.Errorf("invalid info hash %q", infoHash)
	}
	if _, err := hex.DecodeString(infoHash); err != nil {
		return "", fmt.Errorf("invalid info hash %q", infoHash)
	}
	return filepath.Join(dir, infoHash+".json"), nil
}

func SaveBlueprint(dir string, b *Blueprint) error {
	if err := ValidateBlueprint(b); err != nil {
		return err
	}
	path, err := BlueprintPath(dir, b.InfoHash)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	payload, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, payload, 0o644)
}

func LoadBlueprint(dir, infoHash string) (*Blueprint, error) {
	path, err := BlueprintPath(dir, infoHash)
	if err != nil {
		return nil, err
	}

	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var b Blueprint
	if err := json.Unmarshal(payload, &b); err != nil {
		return nil, err
	}
	if err := ValidateBlueprint(&b); err != nil {
		return nil, err
	}
	return &b, nil
}
