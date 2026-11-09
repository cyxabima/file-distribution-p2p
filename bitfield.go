package main

import (
	"sync"
)

// Why we are not doing []bool cuz if 1024 chunks we need 1024 byte for bool but we can do bit level thing
// as 1 bit is enough to keep record wherether chunck is avaliable or not
// why not []byte in which each bit holds the chunk avaliablity record
// this reduce space size by 8  as 1024 / 8 only 128 bytes
type Bitfield struct{
	data []byte
	mu sync.RWMutex
}

func (bf *Bitfield) Has(index uint32) bool  {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	byteIndex := index / 8
	bitOffset := index % 8
	if byteIndex >= uint32((len(bf.data))) {
		return false
	}

	// bit wise and to check the specifc bit is one or not since (1 << bitOffset) garntees that
	// there will be only one bit which is 1 and that bit is of index and rest of bit in byte are zero so "and" will result true
	// if the data has same bit as one 
	return bf.data[byteIndex] & (1<<bitOffset) != 0
}

func (bf *Bitfield) Set(index uint32){
	bf.mu.Lock()
	defer bf.mu.Unlock()
	
	byteIndex := index / 8
	bitOffset := index % 8

	// bitwise Or to flip specifc bit to one 
	bf.data[byteIndex] |= 1<< bitOffset 
}

func (bf *Bitfield) Clear(index uint32){
	bf.mu.Lock()
	defer bf.mu.Unlock()

	byteIndex := index / 8 
	bitOffset := index % 8

	// bitwise nand 
	bf.data[byteIndex] &^= 1<< bitOffset

}
