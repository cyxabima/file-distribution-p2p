package main

import (
	"fmt"
	"io"
	"os"
	"sync"
)

type DiskManger struct {
	file      *os.File
	mu        sync.RWMutex
	chuckSize uint32
	readOnly  bool
}

// OpenDiskManger opens an existing file read-only. A seeder serves chunks
// from the original file, so os.Create (which truncates) would destroy it.
func OpenDiskManger(path string, totalSize int64, chunkSize uint32) (*DiskManger, error) {
	if chunkSize == 0 {
		return nil, fmt.Errorf("chunk size must be greater than zero")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() != totalSize {
		_ = file.Close()
		return nil, fmt.Errorf("file %s is %d bytes, blueprint says %d", path, info.Size(), totalSize)
	}

	return &DiskManger{
		file:      file,
		chuckSize: chunkSize,
		readOnly:  true,
	}, nil
}

func NewDiskManger(path string, totalSize int64, chuckSize uint32) (*DiskManger, error) {
	if chuckSize == 0 {
		return nil, fmt.Errorf("chunk size must be greater than zero")
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// pre allocate the file size on disk
	if err := file.Truncate(totalSize); err != nil {
		_ = file.Close()
		return nil, err
	}

	return &DiskManger{
		file:      file,
		chuckSize: chuckSize,
	}, nil
}

func (dm *DiskManger) WriteChunk(index uint32, data []byte) error {
	if dm.readOnly {
		return fmt.Errorf("cannot write to a read-only seed file")
	}
	if len(data) > int(dm.chuckSize) {
		return fmt.Errorf("chunk %d is %d bytes, larger than chunk size %d", index, len(data), dm.chuckSize)
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	offset := int64(dm.chuckSize * index)
	_, err := dm.file.WriteAt(data, offset)
	return err
}

func (dm *DiskManger) ReadChunk(index uint32) ([]byte, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	offset := int64(dm.chuckSize) * int64(index)
	buf := make([]byte, dm.chuckSize)

	n, err := dm.file.ReadAt(buf, offset)

	// The last chunk is shorter than chunkSize. ReadAt fills what it can and
	// returns io.EOF alongside a positive n; that is a valid chunk, not a
	// failed read.
	if n > 0 && err == io.EOF {
		return buf[:n], nil
	}
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// Close flushes and releases the underlying file.
func (dm *DiskManger) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.file == nil {
		return nil
	}
	return dm.file.Close()
}
