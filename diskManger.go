package main

import (
	"os"
	"sync"
)

type DiskManger struct {
	file      *os.File
	mu        sync.RWMutex
	chuckSize uint32
}

func NewDiskManger(path string, totalSize int64, chuckSize uint32) (*DiskManger, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// pre allocate the file size on disk
	if err := file.Truncate(totalSize); err != nil {
		return nil, err
	}

	return &DiskManger{
		file:      file,
		chuckSize: chuckSize,
	}, nil
}

func (dm *DiskManger) WriteChunk(index uint32, data []byte) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	offset := int64(dm.chuckSize * index)
	_, err := dm.file.WriteAt(data, offset)
	return err
}

func (dm *DiskManger) ReadChunk(index uint32) ([]byte, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	offset := int64(dm.chuckSize * index)
	buf := make([]byte, dm.chuckSize)
	_, err := dm.file.ReadAt(buf, offset)
	// NOTE: Handle io.EOF if the last chunk is smaller than chunkSize
	return buf, err

}
