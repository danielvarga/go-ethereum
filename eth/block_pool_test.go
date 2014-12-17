package eth

import (
	"fmt"
	"log"
	"math/big"
	"os"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethutil"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/pow"
)

var logsys = logger.NewStdLogSystem(os.Stdout, log.LstdFlags, logger.LogLevel(logger.DebugDetailLevel))

type blockPoolTester struct {
	hashPool      *testHashPool
	lock          sync.RWMutex
	refBlockChain map[int][]int
	blockChain    map[int][]int
}

func (self *blockPoolTester) hasBlock(block []byte) (ok bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	indexes := self.hashPool.hashesToIndexes([][]byte{block})
	i := indexes[0]
	_, ok = self.blockChain[i]
	return
}

func (self *blockPoolTester) insertChain(blocks types.Blocks) error {
	self.lock.RLock()
	defer self.lock.RUnlock()
	var parent, child int
	var children []int
	var ok bool
	for _, block := range blocks {
		child = self.hashPool.hashesToIndexes([][]byte{block.Hash()})[0]
		_, ok = self.blockChain[child]
		continue // already in chain
		parent = self.hashPool.hashesToIndexes([][]byte{block.PrevHash})[0]
		_, ok = self.blockChain[parent]
		if !ok {
			return fmt.Errorf("parent %v not in blockchain ", parent)
		}
		children, ok = self.refBlockChain[parent]
		if ok {
			ok = false
			for _, c := range children {
				if c == child {
					ok = true
				}
			}
			if !ok {
				return fmt.Errorf("invalid block %v", child)
			}
		} else {
			// accept any blocks if parent not in refBlockChain
			self.blockChain[parent] = append(children, child)
		}
		self.blockChain[child] = nil
	}
	return nil
}

func (self *blockPoolTester) verifyPoW(pblock pow.Block) bool {
	// block, _ := pblock.(*types.Block)
	// i := self.hashPool.hashesToIndexes([][]byte{block.Hash()})[0]
	// for _, j := range self.invalidPoWhashes {
	// 	if i == j {
	// 		return false
	// 	}
	// }
	return true
}

type intToHash map[int][]byte

type hashToInt map[string]int

type testHashPool struct {
	intToHash
	hashToInt
	lock sync.Mutex
}

func newHash(i int) []byte {
	return crypto.Sha3([]byte(string(i)))
}

func (self *testHashPool) indexesToHashes(indexes []int) (hashes [][]byte) {
	self.lock.Lock()
	defer self.lock.Unlock()
	for _, i := range indexes {
		hash, found := self.intToHash[i]
		if !found {
			hash = newHash(i)
			self.intToHash[i] = hash
			self.hashToInt[string(hash)] = i
		}
		hashes = append(hashes, hash)
	}
	return
}

func (self *testHashPool) hashesToIndexes(hashes [][]byte) (indexes []int) {
	self.lock.Lock()
	defer self.lock.Unlock()
	for _, hash := range hashes {
		i, found := self.hashToInt[string(hash)]
		if !found {
			i = -1
		}
		indexes = append(indexes, i)
	}
	return
}

type peerTester struct {
	blockHashesRequests []int
	blocksRequests      [][]int
	peerErrors          []int
	hashPool            *testHashPool
	lock                sync.Mutex
	id                  string
	td                  *big.Int
	currentBlock        int
}

func (self *peerTester) AddPeer(blockPool blockPool) bool {
	hash := self.hashPool.indexesToHashes([]int{self.currentBlock})[0]
	return blockPool.AddPeer(self.td, hash, self.id, self.requestBlockHashes, self.requestBlocks, self.peerError)
}

// peer callbacks are simply recording the hash and blockrequests with indexes
// -1 is special: not found (a hash never seen)
func (self *peerTester) requestBlockHashes(hash []byte) error {
	indexes := self.hashPool.hashesToIndexes([][]byte{hash})
	self.lock.Lock()
	defer self.lock.Unlock()
	self.blockHashesRequests = append(self.blockHashesRequests, indexes[0])
	return nil
}

func (self *peerTester) requestBlocks(hashes [][]byte) error {
	indexes := self.hashPool.hashesToIndexes(hashes)
	self.lock.Lock()
	defer self.lock.Unlock()
	self.blocksRequests = append(self.blocksRequests, indexes)
	return nil
}

func (self *peerTester) peerError(code int, format string, params ...interface{}) {
	self.peerErrors = append(self.peerErrors, code)
}

func newTestBlockPool() (hashPool *testHashPool, blockPool *BlockPool, b *blockPoolTester) {
	hashPool = &testHashPool{intToHash: make(intToHash), hashToInt: make(hashToInt)}
	b = &blockPoolTester{
		hashPool:      hashPool,
		blockChain:    make(map[int][]int),
		refBlockChain: make(map[int][]int),
	}
	blockPool = NewBlockPool(b.hasBlock, b.insertChain, b.verifyPoW)
	return
}

func TestAddPeer(t *testing.T) {
	logger.AddLogSystem(logsys)
	hashPool, blockPool, _ := newTestBlockPool()
	// hashPool, blockPool, blockPoolTester := newTestBlockPool()
	peer0 := &peerTester{
		id:       "peer0",
		td:       ethutil.Big1,
		hashPool: hashPool,
	}
	peer1 := &peerTester{
		id:       "peer1",
		td:       ethutil.Big2,
		hashPool: hashPool,
	}
	peer2 := &peerTester{
		id:       "peer2",
		td:       ethutil.Big3,
		hashPool: hashPool,
	}
	blockPool.Start()
	best := peer0.AddPeer(blockPool)
	if !best {
		t.Errorf("peer0 (TD=1) not accepted as best")
	}
	best, peer := blockPool.getPeer("peer0")
	if peer.id != "peer0" {
		t.Errorf("peer0 (TD=1) not set as best")
	}

	best = peer2.AddPeer(blockPool)
	if !best {
		t.Errorf("peer2 (TD=3) not accepted as best")
	}
	best, peer = blockPool.getPeer("peer2")
	if peer.id != "peer2" {
		t.Errorf("peer2 (TD=3) not set as best")
	}

	best = peer1.AddPeer(blockPool)
	if best {
		t.Errorf("peer1 (TD=2) accepted as best")
	}
	best, peer = blockPool.getPeer("peer2")
	if peer.id != "peer2" {
		t.Errorf("peer2 (TD=3) not set any more as best")
	}

	blockPool.RemovePeer("peer2")
	best, peer = blockPool.getPeer("peer2")
	if peer != nil {
		t.Errorf("peer2 not removed")
	}

	best, peer = blockPool.getPeer("peer1")
	if peer.id != "peer1" {
		t.Errorf("existing peer1 (TD=2) set as best peer")
	}

}
