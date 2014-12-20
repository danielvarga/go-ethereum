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
	blockPool     *BlockPool
	t             *testing.T
}

func (self *blockPoolTester) hasBlock(block []byte) (ok bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	indexes := self.hashPool.hashesToIndexes([][]byte{block})
	i := indexes[0]
	_, ok = self.blockChain[i]
	fmt.Printf("has block %v (%x...): %v\n", i, block[0:4], ok)
	return
}

func (self *blockPoolTester) insertChain(blocks types.Blocks) error {
	self.lock.RLock()
	defer self.lock.RUnlock()
	var parent, child int
	var children, refChildren []int
	var ok bool
	for _, block := range blocks {
		child = self.hashPool.hashesToIndexes([][]byte{block.Hash()})[0]
		_, ok = self.blockChain[child]
		if ok {
			fmt.Printf("block %v already in blockchain\n", child)
			continue // already in chain
		}
		parent = self.hashPool.hashesToIndexes([][]byte{block.PrevHash})[0]
		children, ok = self.blockChain[parent]
		if !ok {
			return fmt.Errorf("parent %v not in blockchain ", parent)
		}
		ok = false
		var found bool
		refChildren, found = self.refBlockChain[parent]
		if found {
			for _, c := range refChildren {
				if c == child {
					ok = true
				}
			}
			if !ok {
				return fmt.Errorf("invalid block %v", child)
			}
		} else {
			ok = true
		}
		if ok {
			// accept any blocks if parent not in refBlockChain
			fmt.Errorf("blockchain insert %v -> %v\n", parent, child)
			self.blockChain[parent] = append(children, child)
			self.blockChain[child] = nil
		}
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

func arrayEq(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func (self *blockPoolTester) checkBlockChain(blockChain map[int][]int) {
	for k, v := range self.blockChain {
		fmt.Printf("got: %v -> %v\n", k, v)
	}
	for k, v := range blockChain {
		fmt.Printf("expected: %v -> %v\n", k, v)
	}
	if len(blockChain) != len(self.blockChain) {
		self.t.Errorf("blockchain incorrect (length differ)")
	}
	for k, v := range blockChain {
		vv, ok := self.blockChain[k]
		if !ok || !arrayEq(v, vv) {
			self.t.Errorf("blockchain incorrect on %v -> %v (!= %v)", k, vv, v)
		}
	}

}

func (self *peerTester) checkBlocksRequests(blocksRequests ...[]int) {
	if len(blocksRequests) > len(self.blocksRequests) {
		self.t.Errorf("blocks requests incorrect (length differ)\ngot %v\nexpected %v", blocksRequests, self.blocksRequests)
	}
	for i, r := range blocksRequests {
		rr := self.blocksRequests[i]
		if !arrayEq(r, rr) {
			self.t.Errorf("blocks requests incorrect\ngot %v\nexpected", r, rr)
		}
	}
}

func (self *peerTester) checkBlockHashesRequests(blocksHashesRequests ...int) {
	r := blocksHashesRequests
	rr := self.blockHashesRequests
	if len(r) != len(rr) {
		self.t.Errorf("block hashes requests incorrect (length differ)\ngot %v\nexpected", r, rr)
	}
	if !arrayEq(r, rr) {
		self.t.Errorf("block hashes requests incorrect\ngot %v\nexpected", r, rr)
	}
}

func (self *blockPoolTester) newPeer(id string, td int, cb int) *peerTester {
	return &peerTester{
		id:           id,
		td:           big.NewInt(int64(td)),
		currentBlock: cb,
		hashPool:     self.hashPool,
		blockPool:    self.blockPool,
		t:            self.t,
	}
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
	blockPool           blockPool
	hashPool            *testHashPool
	lock                sync.Mutex
	id                  string
	td                  *big.Int
	currentBlock        int
	t                   *testing.T
}

func (self *peerTester) AddPeer() bool {
	hash := self.hashPool.indexesToHashes([]int{self.currentBlock})[0]
	return self.blockPool.AddPeer(self.td, hash, self.id, self.requestBlockHashes, self.requestBlocks, self.peerError)
}

func (self *peerTester) AddBlockHashes(indexes []int) {
	i := 0
	hashes := self.hashPool.indexesToHashes(indexes)
	next := func() (hash []byte, ok bool) {
		if i < len(hashes) {
			hash = hashes[i]
			ok = true
			i++
		}
		return
	}
	self.blockPool.AddBlockHashes(next, self.id)
}

func (self *peerTester) AddBlocks(indexes ...int) {
	hashes := self.hashPool.indexesToHashes(indexes)
	for i := 1; i < len(hashes); i++ {
		self.blockPool.AddBlock(&types.Block{HeaderHash: ethutil.Bytes(hashes[i]), PrevHash: ethutil.Bytes(hashes[i-1])}, self.id)
	}
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

func newTestBlockPool(t *testing.T) (hashPool *testHashPool, blockPool *BlockPool, b *blockPoolTester) {
	hashPool = &testHashPool{intToHash: make(intToHash), hashToInt: make(hashToInt)}
	b = &blockPoolTester{
		t:             t,
		hashPool:      hashPool,
		blockChain:    make(map[int][]int),
		refBlockChain: make(map[int][]int),
	}
	b.blockPool = NewBlockPool(b.hasBlock, b.insertChain, b.verifyPoW)
	blockPool = b.blockPool
	return
}

func TestAddPeer(t *testing.T) {
	logger.AddLogSystem(logsys)
	_, blockPool, blockPoolTester := newTestBlockPool(t)
	peer0 := blockPoolTester.newPeer("peer0", 1, 0)
	peer1 := blockPoolTester.newPeer("peer1", 2, 1)
	peer2 := blockPoolTester.newPeer("peer2", 3, 2)
	blockPool.Start()
	best := peer0.AddPeer()
	if !best {
		t.Errorf("peer0 (TD=1) not accepted as best")
	}
	peer, best := blockPool.getPeer("peer0")
	if peer.id != "peer0" {
		t.Errorf("peer0 (TD=1) not set as best")
	}

	best = peer2.AddPeer()
	if !best {
		t.Errorf("peer2 (TD=3) not accepted as best")
	}
	peer, best = blockPool.getPeer("peer2")
	if peer.id != "peer2" {
		t.Errorf("peer2 (TD=3) not set as best")
	}

	best = peer1.AddPeer()
	if best {
		t.Errorf("peer1 (TD=2) accepted as best")
	}
	peer, best = blockPool.getPeer("peer2")
	if peer.id != "peer2" {
		t.Errorf("peer2 (TD=3) not set any more as best")
	}

	blockPool.RemovePeer("peer2")
	peer, best = blockPool.getPeer("peer2")
	if peer != nil {
		t.Errorf("peer2 not removed")
	}

	peer, best = blockPool.getPeer("peer1")
	if peer.id != "peer1" {
		t.Errorf("existing peer1 (TD=2) set as best peer")
	}

	blockPool.RemovePeer("peer1")
	peer, best = blockPool.getPeer("peer1")
	if peer != nil {
		t.Errorf("peer1 not removed")
	}

	peer, best = blockPool.getPeer("peer0")
	if peer.id != "peer0" {
		t.Errorf("existing peer0 (TD=1) set as best peer")
	}

	blockPool.RemovePeer("peer0")
	peer, best = blockPool.getPeer("peer0")
	if peer != nil {
		t.Errorf("peer1 not removed")
	}

	// adding back earlier peer ok
	peer0.currentBlock = 3
	best = peer0.AddPeer()
	if !best {
		t.Errorf("peer0 (TD=1) not accepted as best")
	}

	peer, best = blockPool.getPeer("peer0")
	if peer.id != "peer0" {
		t.Errorf("peer0 (TD=1) not set as best")
	}

	peer0.checkBlockHashesRequests(0, 0, 3)

	peer1.checkBlockHashesRequests(1)

	peer2.checkBlockHashesRequests(2)

	blockPool.Stop()

}

func TestPeerWithKnownBlock(t *testing.T) {
	logger.AddLogSystem(logsys)
	_, blockPool, blockPoolTester := newTestBlockPool(t)
	blockPoolTester.refBlockChain[0] = nil
	blockPoolTester.blockChain[0] = nil
	// hashPool, blockPool, blockPoolTester := newTestBlockPool()
	blockPool.Start()

	peer0 := blockPoolTester.newPeer("0", 1, 0)
	peer0.AddPeer()
	blockPool.Stop()
	// no request on known block
	peer0.checkBlockHashesRequests()
}

const cycleWait = 10

func TestSimpleChain(t *testing.T) {
	logger.AddLogSystem(logsys)
	_, blockPool, blockPoolTester := newTestBlockPool(t)
	blockPoolTester.blockChain[0] = nil
	for k, v := range blockPoolTester.blockChain {
		fmt.Printf("%v -> %v", k, v)
	}
	blockPoolTester.refBlockChain[0] = []int{1}
	blockPoolTester.refBlockChain[1] = []int{2}
	blockPool.Start()

	peer1 := blockPoolTester.newPeer("peer1", 1, 2)
	peer1.AddPeer()
	peer1.checkBlockHashesRequests(2)
	peer1.AddBlockHashes([]int{2, 1, 0})
	blockPool.cycle(cycleWait)
	blockPool.cycle(cycleWait)
	peer1.checkBlocksRequests([]int{1, 2})
	peer1.AddBlocks(0, 1, 2)
	blockPool.cycle(cycleWait)
	blockPoolTester.refBlockChain[2] = []int{}
	blockPoolTester.checkBlockChain(blockPoolTester.refBlockChain)
	blockPool.Stop()
}
