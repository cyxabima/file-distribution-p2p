package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
)

// GenerateHash returns the SHA-256 of each chunk of a file. The last chunk is
// short, so it must be hashed from buf[:n] rather than the whole buffer.
func GenerateHash(path string, chunkSize uint32) ([][]byte, error) {
	if chunkSize == 0 {
		return nil, fmt.Errorf("chunk size must be greater than zero")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := make([]byte, chunkSize)
	var hashes [][]byte

	for {
		n, err := io.ReadFull(file, buf)
		if n > 0 {
			sum := sha256.Sum256(buf[:n])
			hash := make([]byte, sha256.Size)
			copy(hash, sum[:]) // copy INTO the new slice, not into sum
			hashes = append(hashes, hash)
		}

		switch err {
		case io.EOF, io.ErrUnexpectedEOF:
			return hashes, nil // a short final chunk is normal
		case nil:
			continue
		default:
			return nil, err
		}
	}
}
