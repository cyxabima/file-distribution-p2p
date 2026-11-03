package p2p

type Discoverer interface {
	Discover() <-chan string
}
