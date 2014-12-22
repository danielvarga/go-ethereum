package eth

import (
	"fmt"
	"log"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

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

func (self *blockPoolTester) Errorf(format string, params ...interface{}) {
	fmt.Printf(format+"\n", params...)
	self.t.Errorf(format, params...)
}

func (self *peerTester) Errorf(format string, params ...interface{}) {
	fmt.Printf(format+"\n", params...)
	self.t.Errorf(format, params...)
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
		self.Errorf("blockchain incorrect (zlength differ)")
	}
	for k, v := range blockChain {
		vv, ok := self.blockChain[k]
		if !ok || !arrayEq(v, vv) {
			self.Errorf("blockchain incorrect on %v -> %v (!= %v)", k, vv, v)
		}
	}

}

func (self *peerTester) checkBlocksRequests(blocksRequests ...[]int) {
	if len(blocksRequests) > len(self.blocksRequests) {
		self.Errorf("blocks requests incorrect (length differ)\ngot %v\nexpected %v", self.blocksRequests, blocksRequests)
	} else {
		for i, rr := range blocksRequests {
			r := self.blocksRequests[i]
			if !arrayEq(r, rr) {
				self.Errorf("blocks requests incorrect\ngot %v\nexpected %v", self.blocksRequests, blocksRequests)
			}
		}
	}
}

func (self *peerTester) waitBlocksRequests(blocksRequest ...int) {
	rr := blocksRequest
	self.lock.RLock()
	r := self.blocksRequestsMap
	self.lock.RUnlock()
	fmt.Printf("blocks request check %v (%v)\n", rr, r)
	for {
		self.lock.RLock()
		i := 0
		for i = 0; i < len(rr); i++ {
			r = self.blocksRequestsMap
			_, ok := r[rr[i]]
			if !ok {
				break
			}
		}
		self.lock.RUnlock()

		if i == len(rr) {
			return
		}
		fmt.Printf("waiting for blocks request %v (%v)\n", rr, r)
		ok, err := self.blockPool.HashCycle(cycleWait * time.Second)
		if err != nil {
			self.t.Errorf("%v", err)
		}
		if !ok {
			time.Sleep(100 * time.Millisecond)
		}

	}
}

func (self *peerTester) checkBlockHashesRequests(blocksHashesRequests ...int) {
	rr := blocksHashesRequests
	self.lock.RLock()
	r := self.blockHashesRequests
	self.lock.RUnlock()
	if len(r) != len(rr) {
		self.Errorf("block hashes requests incorrect (length differ)\ngot %v\nexpected %v", r, rr)
	} else {
		if !arrayEq(r, rr) {
			self.Errorf("block hashes requests incorrect\ngot %v\nexpected %v", r, rr)
		}
	}
}

func (self *peerTester) waitBlockHashesRequests(blocksHashesRequest int) {
	rr := blocksHashesRequest
	self.lock.RLock()
	r := self.blockHashesRequests
	self.lock.RUnlock()
	fmt.Printf("[%s] block hash request check %v (%v)\n", self.id, rr, r)
	n := 0
	i := 0
	for {
		for i = n; i < len(r); i++ {
			if rr == r[i] {
				return
			}
		}
		n = i
		fmt.Printf("waiting for block hash request %v (%v)\n", rr, r)
		ok, err := self.blockPool.HashCycle(cycleWait * time.Second)
		if err != nil {
			self.t.Errorf("%v", err)
		}
		if !ok {
			time.Sleep(100 * time.Millisecond)
		}
		self.lock.RLock()
		r = self.blockHashesRequests
		self.lock.RUnlock()
	}
}

func (self *blockPoolTester) initRefBlockChain(n int) {
	for i := 0; i < n; i++ {
		self.refBlockChain[i] = []int{i + 1}
	}
}

func (self *blockPoolTester) newPeer(id string, td int, cb int) *peerTester {
	return &peerTester{
		id:                id,
		td:                td,
		currentBlock:      cb,
		hashPool:          self.hashPool,
		blockPool:         self.blockPool,
		t:                 self.t,
		blocksRequestsMap: make(map[int]bool),
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
	blocksRequestsMap   map[int]bool
	peerErrors          []int
	blockPool           *BlockPool
	hashPool            *testHashPool
	lock                sync.RWMutex
	id                  string
	td                  int
	currentBlock        int
	t                   *testing.T
}

func (self *peerTester) AddPeer() bool {
	hash := self.hashPool.indexesToHashes([]int{self.currentBlock})[0]
	return self.blockPool.AddPeer(big.NewInt(int64(self.td)), hash, self.id, self.requestBlockHashes, self.requestBlocks, self.peerError)
}

func (self *peerTester) AddBlockHashes(indexes ...int) {
	i := 0
	fmt.Printf("ready to add block hashes %v\n", indexes)

	self.waitBlockHashesRequests(indexes[0])
	fmt.Printf("adding block hashes %v\n", indexes)
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
	fmt.Printf("ready to add blocks %v\n", indexes[1:])
	self.waitBlocksRequests(indexes[1:]...)
	fmt.Printf("adding blocks %v \n", indexes[1:])
	for i := 1; i < len(hashes); i++ {
		fmt.Printf("adding block %v %x\n", indexes[i], hashes[i][:4])
		self.blockPool.AddBlock(&types.Block{HeaderHash: ethutil.Bytes(hashes[i]), PrevHash: ethutil.Bytes(hashes[i-1])}, self.id)
	}
}

// peer callbacks are simply recording the hash and blockrequests with indexes
// -1 is special: not found (a hash never seen)
func (self *peerTester) requestBlockHashes(hash []byte) error {
	indexes := self.hashPool.hashesToIndexes([][]byte{hash})
	fmt.Printf("[%s] blocks hash request %v %x\n", self.id, indexes[0], hash[:4])
	self.lock.Lock()
	defer self.lock.Unlock()
	self.blockHashesRequests = append(self.blockHashesRequests, indexes[0])
	return nil
}

func (self *peerTester) requestBlocks(hashes [][]byte) error {
	indexes := self.hashPool.hashesToIndexes(hashes)
	fmt.Printf("blocks request %v %x...\n", indexes, hashes[0][:4])
	self.lock.Lock()
	defer self.lock.Unlock()
	self.blocksRequests = append(self.blocksRequests, indexes)
	for _, i := range indexes {
		self.blocksRequestsMap[i] = true
	}
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
	// logger.AddLogSystem(logsys)
	_, blockPool, blockPoolTester := newTestBlockPool(t)
	peer0 := blockPoolTester.newPeer("peer0", 1, 0)
	peer1 := blockPoolTester.newPeer("peer1", 2, 1)
	peer2 := blockPoolTester.newPeer("peer2", 3, 2)
	blockPool.Start()
	var peer *peerInfo
	best := peer0.AddPeer()
	if !best {
		t.Errorf("peer0 (TD=1) not accepted as best")
	}
	if blockPool.peer.id != "peer0" {
		t.Errorf("peer0 (TD=1) not set as best")
	}
	peer0.checkBlockHashesRequests(0)

	best = peer2.AddPeer()
	if !best {
		t.Errorf("peer2 (TD=3) not accepted as best")
	}
	if blockPool.peer.id != "peer2" {
		t.Errorf("peer2 (TD=3) not set as best")
	}
	peer2.checkBlockHashesRequests(2)

	best = peer1.AddPeer()
	if best {
		t.Errorf("peer1 (TD=2) accepted as best")
	}
	if blockPool.peer.id != "peer2" {
		t.Errorf("peer2 (TD=3) not set any more as best")
	}
	if blockPool.peer.td.Cmp(big.NewInt(int64(3))) != 0 {
		t.Errorf("peer1 TD not set")
	}

	peer2.td = 4
	peer2.currentBlock = 3
	best = peer2.AddPeer()
	if !best {
		t.Errorf("peer2 (TD=4) not accepted as best")
	}
	if blockPool.peer.id != "peer2" {
		t.Errorf("peer2 (TD=4) not set as best")
	}
	if blockPool.peer.td.Cmp(big.NewInt(int64(4))) != 0 {
		t.Errorf("peer2 TD not updated")
	}
	peer2.checkBlockHashesRequests(2, 3)

	peer1.td = 3
	peer1.currentBlock = 2
	best = peer1.AddPeer()
	if best {
		t.Errorf("peer1 (TD=3) should not be set as best")
	}
	if blockPool.peer.id == "peer1" {
		t.Errorf("peer1 (TD=3) should not be set as best")
	}
	peer, best = blockPool.getPeer("peer1")
	if peer.td.Cmp(big.NewInt(int64(3))) != 0 {
		t.Errorf("peer1 TD should be updated")
	}

	blockPool.RemovePeer("peer2")
	peer, best = blockPool.getPeer("peer2")
	if peer != nil {
		t.Errorf("peer2 not removed")
	}

	if blockPool.peer.id != "peer1" {
		t.Errorf("existing peer1 (TD=3) should not be set as best peer")
	}
	peer1.checkBlockHashesRequests(1, 2)

	blockPool.RemovePeer("peer1")
	peer, best = blockPool.getPeer("peer1")
	if peer != nil {
		t.Errorf("peer1 not removed")
	}

	if blockPool.peer.id != "peer0" {
		t.Errorf("existing peer0 (TD=1) should be set as best peer")
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
		t.Errorf("peer0 (TD=1) should be set as best")
	}

	if blockPool.peer.id != "peer0" {
		t.Errorf("peer0 (TD=1) should be set as best")
	}
	peer0.checkBlockHashesRequests(0, 0, 3)

	blockPool.Stop()

}

func TestPeerWithKnownBlock(t *testing.T) {
	// logger.AddLogSystem(logsys)
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
	blockPoolTester.initRefBlockChain(2)
	blockPool.Start()

	peer1 := blockPoolTester.newPeer("peer1", 1, 2)
	peer1.AddPeer()
	peer1.AddBlockHashes(2, 1, 0)
	peer1.AddBlocks(0, 1, 2)
	blockPool.Wait(cycleWait)
	blockPool.Stop()
	blockPoolTester.refBlockChain[2] = []int{}
	blockPoolTester.checkBlockChain(blockPoolTester.refBlockChain)
}

func TestMultiSectionChain(t *testing.T) {
	logger.AddLogSystem(logsys)
	_, blockPool, blockPoolTester := newTestBlockPool(t)
	blockPoolTester.blockChain[0] = nil
	blockPoolTester.initRefBlockChain(5)
	blockPool.Start()

	peer1 := blockPoolTester.newPeer("peer1", 1, 5)
	peer1.AddPeer()
	peer1.AddBlockHashes(5, 4, 3)
	peer1.AddBlocks(2, 3, 4, 5)
	peer1.AddBlockHashes(3, 2, 1, 0)
	peer1.AddBlocks(0, 1, 2)
	blockPool.Wait(cycleWait * time.Second)
	blockPool.Stop()
	blockPoolTester.refBlockChain[5] = []int{}
	blockPoolTester.checkBlockChain(blockPoolTester.refBlockChain)
}

func TestMidChainNewBlock(t *testing.T) {
	// logger.AddLogSystem(logsys)
	_, blockPool, blockPoolTester := newTestBlockPool(t)
	blockPoolTester.blockChain[0] = nil
	blockPoolTester.initRefBlockChain(6)
	blockPool.Start()

	peer1 := blockPoolTester.newPeer("peer1", 1, 4)
	peer1.AddPeer()
	go peer1.AddBlockHashes(4, 3)
	go peer1.AddBlocks(2, 3, 4)
	peer1.td = 2
	peer1.currentBlock = 6
	peer1.AddPeer()
	go peer1.AddBlockHashes(6, 5, 4)
	go peer1.AddBlocks(4, 5, 6)
	go peer1.AddBlockHashes(3, 2, 1, 0)
	go peer1.AddBlocks(0, 1, 2)
	blockPool.Wait(cycleWait * time.Second)
	blockPool.Stop()
	blockPoolTester.refBlockChain[6] = []int{}
	blockPoolTester.checkBlockChain(blockPoolTester.refBlockChain)
}

func TestMidChainPeerSwitch(t *testing.T) {
	logger.AddLogSystem(logsys)
	_, blockPool, blockPoolTester := newTestBlockPool(t)
	blockPoolTester.blockChain[0] = nil
	blockPoolTester.initRefBlockChain(6)
	blockPool.Start()

	peer1 := blockPoolTester.newPeer("peer1", 1, 4)
	peer1.AddPeer()
	go peer1.AddBlockHashes(4, 3)
	peer1.AddBlocks(2, 3, 4)
	peer2 := blockPoolTester.newPeer("peer2", 2, 6)
	peer2.blocksRequestsMap = peer1.blocksRequestsMap
	peer2.AddPeer()
	go peer2.AddBlockHashes(6, 5, 4)
	go peer2.AddBlocks(3, 4, 5, 6)
	go peer2.AddBlockHashes(3, 2, 1, 0)
	peer2.AddBlocks(0, 1, 2)
	blockPool.Wait(cycleWait * time.Second)
	blockPool.Stop()
	blockPoolTester.refBlockChain[6] = []int{}
	blockPoolTester.checkBlockChain(blockPoolTester.refBlockChain)
}

func TestMidChainPeerDownSwitch(t *testing.T) {
	logger.AddLogSystem(logsys)
	_, blockPool, blockPoolTester := newTestBlockPool(t)
	blockPoolTester.blockChain[0] = nil
	blockPoolTester.initRefBlockChain(6)
	blockPool.Start()

	peer1 := blockPoolTester.newPeer("peer1", 1, 4)
	peer2 := blockPoolTester.newPeer("peer2", 2, 6)
	peer2.blocksRequestsMap = peer1.blocksRequestsMap

	peer2.AddPeer()
	go peer2.AddBlockHashes(6, 5, 4)
	peer2.AddBlocks(3, 4, 5, 6)
	blockPool.RemovePeer("peer2")
	peer1.AddPeer()
	go peer1.AddBlockHashes(4, 3, 2, 1, 0)
	go peer1.AddBlocks(0, 1, 2, 3)
	blockPool.Wait(cycleWait * time.Second)
	blockPool.Stop()
	blockPoolTester.refBlockChain[6] = []int{}
	blockPoolTester.checkBlockChain(blockPoolTester.refBlockChain)
}

func TestMidChainPeerSwitchesBack(t *testing.T) {
	logger.AddLogSystem(logsys)
	_, blockPool, blockPoolTester := newTestBlockPool(t)
	blockPoolTester.blockChain[0] = nil
	blockPoolTester.initRefBlockChain(6)
	blockPool.Start()

	peer1 := blockPoolTester.newPeer("peer1", 1, 4)
	peer2 := blockPoolTester.newPeer("peer2", 2, 6)
	peer2.blocksRequestsMap = peer1.blocksRequestsMap

	peer2.AddPeer()
	go peer2.AddBlockHashes(6, 5, 4)
	peer2.AddBlocks(3, 4, 5, 6)
	blockPool.RemovePeer("peer2")
	peer2.AddPeer()
	go peer2.AddBlockHashes(4, 3, 2, 1, 0)
	go peer2.AddBlocks(0, 1, 2, 3)
	blockPool.Wait(cycleWait * time.Second)
	blockPool.Stop()
	blockPoolTester.refBlockChain[6] = []int{}
	blockPoolTester.checkBlockChain(blockPoolTester.refBlockChain)
}
