package main

import "github.com/cyxabima/minimal-p2p-library-in-go/p2p"

type FileShareApp struct{
	swarm *p2p.SwarmManager
	state *ApplicationState
	disk *DiskManger
	verifier *DataVerifier
}

func NewFileShareApp(listenAddr,filepath string, fileSize int64, chuckSize uint32, expectedHashes [][]byte) (*FileShareApp, error){
	totalChunks := uint32(len(expectedHashes))
	app := FileShareApp{
		state: NewApplicationState(totalChunks),
		verifier: NewDataVerifier(expectedHashes),
	}
	
	disk, err := NewDiskManger(filepath,fileSize, chuckSize)
	if err != nil {
		return nil, err
	}

	app.disk = disk

	transport := p2p.NewTcpTransport(p2p.TCPTransportOpts{
		ListenAddr: listenAddr,
		// TODO: Encoder and Decoder functions
	})

	app.swarm = p2p.NewSwarmManager(p2p.SwarmManagerOpts{
		Transport: transport,
		OnMessage: app.onMessage, // for wiring incoming network 
	})


	return  &app , nil
}


func (app *FileShareApp) onMessage(p2p.Message) error{
	return nil
}
