package main

import (
	"bytes"
	"crypto/sha256"
)

type DataVerifier struct{

	// holds the hash of each chunck; is 32 byte sha256 hash
	expectedHashes [][]byte
}

func NewDataVerifier (hashes [][]byte) *DataVerifier{
	return &DataVerifier{
		expectedHashes: hashes,
	}	
}

func (dv *DataVerifier) Verify(index uint32, data []byte) bool {
	
	if index > uint32(len(dv.expectedHashes)) {
		return false
	}

	computedHash := sha256.Sum256(data)

	return bytes.Equal(computedHash[:], dv.expectedHashes[index])
}

