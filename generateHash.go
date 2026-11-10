package main

import (
	"crypto/sha256"
	"io"
	"os"
)


func GenerateHash(filepath string, chunkSize uint32) ([][]byte, error) {
	file, err := os.Open(filepath)
	if err != nil{
		return nil, err
	}
	defer file.Close()
	
	var hashes [][]byte
	buf:= make([]byte, chunkSize)

	for {
		n, err := file.Read(buf)
		if n > 0 {
			hash := sha256.Sum256(buf)
			hashCopy := make([]byte,32)
			copy(hash[:],hashCopy)
			hashes = append(hashes, hashCopy)
		}
		if err == io.EOF{
			break
		}
		if err != nil {
			return nil, err
		}

	}
	
	return hashes, nil

}
