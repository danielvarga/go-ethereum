package eth

import (
	"math"
	"math/big"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethutil"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/pow"
)

var poolLogger = logger.NewLogger("Blockpool")

const (
	blockHashesBatchSize       = 256
	blockBatchSize             = 64
	blocksRequestInterval      = 10 // seconds
	blocksRequestRepetition    = 1
	blockHashesRequestInterval = 10 // seconds
	blocksRequestMaxIdleRounds = 10
	cacheTimeout               = 3 // minutes
	blockTimeout               = 5 // minutes
)

type poolNode struct {
	lock        sync.RWMutex
	hash        []byte
	td          *big.Int
	block       *types.Block
	child       *poolNode
	parent      *poolNode
	section     *section
	knownParent bool
	peer        string
	source      string
}

type BlockPool struct {
	lock sync.RWMutex

	pool map[string]*poolNode

	peersLock sync.RWMutex
	peers     map[string]*peerInfo
	peer      *peerInfo

	quit    chan bool
	flushC  chan bool
	wg      sync.WaitGroup
	running bool

	cycleGLock  sync.Mutex
	cycleLock   sync.RWMutex
	cycleC      chan chan bool
	cycleN      int
	cycleWaiter chan bool
	cycleWG     sync.WaitGroup

	// the minimal interface with blockchain
	hasBlock    func(hash []byte) bool
	insertChain func(types.Blocks) error
	verifyPoW   func(pow.Block) bool
}

type peerInfo struct {
	lock sync.RWMutex

	td           *big.Int
	currentBlock []byte
	id           string

	requestBlockHashes func([]byte) error
	requestBlocks      func([][]byte) error
	peerError          func(int, string, ...interface{})

	sections map[string]*section
	roots    []*poolNode
	quitC    chan bool
}

func NewBlockPool(hasBlock func(hash []byte) bool, insertChain func(types.Blocks) error, verifyPoW func(pow.Block) bool,
) *BlockPool {
	return &BlockPool{
		hasBlock:    hasBlock,
		insertChain: insertChain,
		verifyPoW:   verifyPoW,
	}
}

// allows restart
func (self *BlockPool) Start() {
	self.lock.Lock()
	if self.running {
		self.lock.Unlock()
		return
	}
	self.running = true
	self.quit = make(chan bool)
	self.flushC = make(chan bool)
	self.cycleWaiter = make(chan bool)
	self.cycleC = make(chan chan bool)
	self.pool = make(map[string]*poolNode)
	self.lock.Unlock()

	self.peersLock.Lock()
	self.peers = make(map[string]*peerInfo)
	self.peersLock.Unlock()

	poolLogger.Infoln("Started")

}

func (self *BlockPool) Stop() {
	self.lock.Lock()
	if !self.running {
		self.lock.Unlock()
		return
	}
	self.running = false
	self.lock.Unlock()

	poolLogger.Infoln("Stopping")

	close(self.quit)
	self.lock.Lock()
	self.peersLock.Lock()
	self.peers = nil
	self.pool = nil
	self.peer = nil
	self.wg.Wait()
	self.lock.Unlock()
	self.peersLock.Unlock()
	poolLogger.Infoln("Stopped")

}

func (self *BlockPool) flush() {
	self.lock.Lock()
	if !self.running {
		self.lock.Unlock()
		return
	}

	poolLogger.Infoln("Waiting")
	self.lock.Unlock()
	close(self.flushC)
	self.flushC = make(chan bool)
	self.wg.Wait()

	poolLogger.Infoln("Stopped")

}

// signal used in testing to trigger and wait until
// at least one cycle of iteration through the chain section is complete
// returns true if at least one process finishes a cycle
// returns false if no processes picked up on the signal within t seconds
func (self *BlockPool) cycle(t time.Duration) (ok bool) {
	// ignore if pool not running
	self.lock.Lock()
	if !self.running {
		self.lock.Unlock()
		return false
	}
	self.lock.Unlock()

	// global cycle lock so that cycle waits cannot be concurrent
	// 	self.cycleGLock.Lock()
	// defer self.cycleGLock.Unlock()
	// lock not needed since it all writes run in a single process?
	cycleC := self.cycleC
	cycleWaiter := self.cycleWaiter
	cycleN := self.cycleN
	self.cycleLock.Lock()
	self.cycleC = make(chan chan bool)
	self.cycleWaiter = make(chan bool)
	self.cycleN++
	self.cycleLock.Unlock()

	poolLogger.Debugf("signal cycle %v", cycleN)

	waiter := make(chan bool)
	poolLogger.Debugf("waiting %v", cycleN)
	go func() {
		self.wg.Wait()
		<-cycleC
		close(waiter)
	}()

	cycleC <- waiter

	select {
	case _, ok := <-waiter:
		if ok {
			poolLogger.Debugf("first process received signal %v", cycleN)
			self.cycleWG.Wait()
		} else {
			poolLogger.Debugf("no processes %v", cycleN)
		}
		poolLogger.Debugf("cycle %v complete", cycleN)
	case <-time.After(t * time.Second):
		poolLogger.Debugf("no processes picked up %v", cycleN)
	}
	close(cycleWaiter)
	return
}

// AddPeer is called by the eth protocol instance running on the peer after
// the status message has been received with total difficulty and current block hash
// AddPeer can only be used once, RemovePeer needs to be called when the peer disconnects
func (self *BlockPool) AddPeer(td *big.Int, currentBlock []byte, peerId string, requestBlockHashes func([]byte) error, requestBlocks func([][]byte) error, peerError func(int, string, ...interface{})) bool {
	self.peersLock.Lock()
	defer self.peersLock.Unlock()
	if self.peers[peerId] != nil {
		panic("peer already added")
	}

	peer := &peerInfo{
		td:                 td,
		currentBlock:       currentBlock,
		id:                 peerId, //peer.Identity().Pubkey()
		requestBlockHashes: requestBlockHashes,
		requestBlocks:      requestBlocks,
		peerError:          peerError,
		sections:           make(map[string]*section),
	}
	self.peers[peerId] = peer
	poolLogger.Debugf("add new peer %v with td %v", peerId, td)

	// check peer current head
	if self.hasBlock(currentBlock) {
		// peer not ahead
		return false
	}
	node := self.get(currentBlock)
	if node == nil {
		// node created if not exist
		node = &poolNode{
			hash: currentBlock,
			peer: peerId,
			td:   td,
		}
		node.section = &section{
			controlC: make(chan bool),
			resetC:   make(chan bool),
			top:      node,
		}
		self.set(currentBlock, node)
	}
	peer.addRoot(node)

	currentTD := ethutil.Big0
	if self.peer != nil {
		currentTD = self.peer.td
	}
	if td.Cmp(currentTD) > 0 {
		if self.peer != nil {
			self.peer.stop(peer)
		}
		poolLogger.Debugf("peer %v promoted to best peer", peerId)
		peer.start(self.peer)
		self.peer = peer
		return true
	}
	return false
}

// RemovePeer is called by the eth protocol when the peer disconnects
func (self *BlockPool) RemovePeer(peerId string) {
	self.peersLock.Lock()
	defer self.peersLock.Unlock()
	peer, ok := self.peers[peerId]
	if !ok {
		return
	}
	delete(self.peers, peerId)
	poolLogger.Debugf("remove peer %v", peerId)

	// if current best peer is removed, need find a better one
	if self.peer == peer {
		var newPeer *peerInfo
		max := ethutil.Big0
		// peer with the highest self-acclaimed TD is chosen
		for _, info := range self.peers {
			if info.td.Cmp(max) > 0 {
				max = info.td
				newPeer = info
			}
		}
		self.peer = newPeer
		peer.stop(newPeer)
		if newPeer != nil {
			poolLogger.Infof("peer %v with td %v promoted to best peer", newPeer.id, newPeer.td)
			newPeer.start(peer)
		} else {
			poolLogger.Warnln("no peers left")
		}
	}
}

// Entry point for eth protocol to add block hashes received via BlockHashesMsg
// only hashes from the best peer is handled
// this method is always responsible to initiate further hash requests until
// a known parent is reached unless cancelled by a peerChange event
// this process also launches all request processes on each chain section
// this function needs to run asynchronously for one peer since the message is discarded???
func (self *BlockPool) AddBlockHashes(next func() ([]byte, bool), peerId string) {

	// check if this peer is the best
	peer, best := self.getPeer(peerId)
	if !best {
		return
	}
	// peer is still the best
	poolLogger.Debugf("adding hashes for best peer %s", peerId)
	self.cycleLock.Lock()
	cycleWaiter := self.cycleWaiter
	cycleC := self.cycleC
	self.cycleLock.Unlock()

	// iterate using next (rlp stream lazy decoder) feeding hashesC
	self.wg.Add(1)
	go func() {
		var child *poolNode
		var depth int
		var firstHash []byte

	LOOP:
		for {

			select {
			case <-self.quit:
				break LOOP
			case <-peer.quitC:
				// if the peer is demoted, no more hashes taken
				break LOOP
			default:
				hash, ok := next()
				poolLogger.Debugf("[%x] depth %v", hash[:4], depth)

				if firstHash == nil {
					firstHash = hash
				}
				if ok && self.hasBlock(hash) {
					// check if known block connecting the downloaded chain to our blockchain
					poolLogger.Debugf("[%x] known block", hash[0:4])
					ok = false
				}
				if !ok {
					if child != nil {
						child.Lock()
						// mark child as absolute pool root with parent known to blockchain
						child.knownParent = true
						child.section.bottom = child
						child.Unlock()
					}
					break LOOP
				}
				// look up node in pool
				parent := self.get(hash)
				if parent != nil {
					poolLogger.Debugf("[%x] found node", hash[:4])
					// reached a known chain in the pool
					if child != nil {
						self.link(parent, child)
						// poolLogger.Debugf("potential chain of %v blocks added", depth)
					} else {
						// we expect the first block to be known, only continue if has no parent
						parent.RLock()
						if parent.parent == nil {
							// the first block hash received is an orphan in the pool, so rejoice and continue
							depth++
							child = parent
							parent.RUnlock()
							continue LOOP
						}
						parent.RUnlock()
					}
					// activate the current chain
					self.activateChain(parent, peer, true)
					poolLogger.Debugf("[%x] reached blockpool, activate chain", hash[:4])
					break LOOP
				}
				// if node for block hash does not exist, create it and index in the pool
				parent = &poolNode{
					hash:  hash,
					child: child,
					peer:  peerId,
				}
				poolLogger.Debugf("[%x] -> orphan %x", hash[:4], child.hash[:4])
				child.Lock()
				if child.child == nil {
					parent.section = child.section
				} else {
					parent.section = &section{
						controlC: make(chan bool),
						resetC:   make(chan bool),
						top:      parent,
					}
				}
				child.parent = parent
				child.Unlock()
				self.set(hash, parent)
				poolLogger.Debugf("[%x] create node", hash[0:4])

				depth++
				child = parent

			} // select
		} //for

		if child != nil {
			poolLogger.Debugf("chain section of %v hashes added %x-%x by peer %s", depth, child.hash[0:4], firstHash[0:4], peerId)
			// start a processSection on the last node, but switch off asking
			// hashes and blocks until next peer confirms this chain
			// or blocks are all received
			self.cycleWG.Add(1)
			waiter, ok := <-cycleC
			if ok {
				waiter <- true
				close(cycleC)
			}
			self.processSection(child)
			self.cycleWG.Done()
			peer.addSection(child.hash, child.section)
			child.section.start()

		}
		poolLogger.Debugf("done adding hashes for peer %s", peerId)
		<-cycleWaiter
		self.wg.Done()
	}()
}

// AddBlock is the entry point for the eth protocol when blockmsg is received upon requests
// It has a strict interpretation of the protocol in that if the block received has not been requested, it results in an error (which can be ignored)
// block is checked for PoW
// only the first PoW-valid block for a hash is considered legit
func (self *BlockPool) AddBlock(block *types.Block, peerId string) {
	hash := block.Hash()
	poolLogger.Debugf("adding block [%x] by peer %s", hash[0:4], peerId)
	if self.hasBlock(hash) {
		poolLogger.Debugf("block [%x] already known", hash[0:4])
		return
	}
	node := self.get(hash)
	if node == nil {
		poolLogger.Debugf("unrequested block [%x] by peer %s", hash[0:4], peerId)
		self.peerError(peerId, ErrUnrequestedBlock, "%x", hash)
		return
	}
	node.RLock()
	b := node.block
	source := node.source
	node.RUnlock()
	if b != nil {
		poolLogger.Debugf("block [%x] already sent by %s", hash[0:4], source)
		return
	}

	// validate block for PoW
	if !self.verifyPoW(block) {
		poolLogger.Debugf("invalid pow on block [%x] by peer %s", hash[0:4], peerId)
		self.peerError(peerId, ErrInvalidPoW, "%x", hash)
		return
	}
	poolLogger.Debugf("added block [%x] by peer %s", hash[0:4], peerId)
	node.Lock()
	node.block = block
	node.source = peerId
	node.Unlock()
}

// iterates down a known poolchain and activates fetching processes
// on each chain section for the peer
// stops if the peer is demoted
// registers last section root as root for the peer (in case peer is promoted a second time, to remember)
func (self *BlockPool) activateChain(node *poolNode, peer *peerInfo, on bool) {
	self.wg.Add(1)
	poolLogger.Debugf("[%x] activate known chain for peer %s", node.hash[0:4], peer.id)

	go func() {
	LOOP:
		for {
			node.sectionRLock()
			bottom := node.section.bottom
			if bottom == nil { // the chain section is being created or killed
				break LOOP
			}
			// register this section with the peer
			if peer != nil {
				peer.addSection(bottom.hash, bottom.section)
				if on {
					// turn on active mode if on == true
					bottom.section.start()
				} else {
					// turn on idle mode if on == false
					bottom.section.stop()
				}
			}
			if bottom.parent == nil {
				node = bottom
				break LOOP
			}
			// if peer demoted stop activation
			select {
			case <-peer.quitC:
				break LOOP
			case <-self.quit:
				break LOOP
			default:
			}

			node = bottom.parent
			bottom.sectionRUnlock()
		}
		// remember root for this peer if on == true
		if on {
			peer.addRoot(node)
		}
		self.wg.Done()
	}()
}

// main worker thread on each section in the poolchain
// - kills the section if there are blocks missing after an absolute time
// - kills the section if there are maxIdleRounds of idle rounds of block requests with no response
// - periodically polls the chain section for missing blocks which are then requested from peers
// - registers the process controller on the peer so that if the peer is promoted as best peer the second time (after a disconnect of a better one), all active processes are switched back on unless they expire and killed ()
// - when turned off (if peer disconnects and new peer connects with alternative chain), no blockrequests are made but absolute expiry timer is ticking
// - when turned back on it recursively calls itself on the root of the next chain section
// - when exits, signals to
func (self *BlockPool) processSection(node *poolNode) {

	node.sectionRLock()
	controlC := node.section.controlC
	resetC := node.section.resetC
	node.sectionRUnlock()

	self.cycleLock.RLock()
	var nextCycleN int = self.cycleN
	var nextCycleC chan chan bool = self.cycleC
	var nextCycleWaiter chan bool = self.cycleWaiter
	self.cycleLock.RUnlock()

	self.wg.Add(1)
	go func() {
		// absolute time after which sub-chain is killed if not complete (some blocks are missing)
		suicideTimer := time.After(blockTimeout * time.Minute)
		var blocksRequestTimer, blockHashesRequestTimer <-chan time.Time
		var missingC, processC, offC chan *poolNode
		var hashes [][]byte
		var i, total, missing, lastMissing, depth int
		var blockHashesRequests, blocksRequests int
		var idle int
		var init, alarm, done, same, running, flush, cycle, cycleStarted, ready, newCycle bool
		orignode := node
		hash := node.hash
		poolLogger.Debugf("[%x] start section process", hash[0:4])
		var cycleC chan chan bool
		var cycleWaiter chan bool
		var cycleN int
		newCycle = true

	LOOP:
		for {

			if newCycle && cycleC == nil && !cycle {
				poolLogger.Debugf("[%x] cycle %v control set (current %v)", hash[0:4], nextCycleN, self.cycleN)
				cycleN = nextCycleN
				cycleC = nextCycleC
				cycleWaiter = nextCycleWaiter
				newCycle = false
			}

			// went through all blocks in section
			if done {
				if missing == 0 {
					// no missing blocks
					poolLogger.Debugf("[%x] got all blocks. process complete (%v total blocksRequests): missing %v/%v/%v", hash[0:4], blocksRequests, missing, total, depth)
					node.sectionLock()
					node.section.complete = true
					node.sectionUnlock()
					blocksRequestTimer = nil
					if blockHashesRequestTimer == nil {
						// not waiting for hashes any more
						poolLogger.Debugf("[%x] block hash request (%v total attempts) not needed", hash[0:4], blockHashesRequests)
						break LOOP
					} // otherwise suicide if no hashes coming
				} else {
					// some missing blocks
					blocksRequests++
					poolLogger.Debugf("[%x] block request attempt %v: missing %v/%v/%v", hash[0:4], blocksRequests, missing, total, depth)
					if len(hashes) > 0 {
						// send block requests to peers
						self.requestBlocks(blocksRequests, hashes)
						hashes = nil
					}
					if missing == lastMissing {
						// idle round
						if same {
							// more than once
							idle++
							// too many idle rounds
							if idle > blocksRequestMaxIdleRounds {
								poolLogger.Debugf("[%x] block requests had %v idle rounds (%v total attempts): missing %v/%v/%v\ngiving up...", hash[0:4], idle, blocksRequests, missing, total, depth)
								self.killChain(node, nil)
								break LOOP
							}
						} else {
							idle = 0
						}
						same = true
					} else {
						same = false
					}
				}
				if flush {
					suicideTimer = time.After(0)
				}
				if cycle {
					if cycleStarted {
						poolLogger.Debugf("[%x] finish cycle", hash[0:4])
						self.cycleWG.Done()
						cycle = false
						cycleStarted = false
						alarm = true
						go func() {
							<-cycleWaiter
							poolLogger.Debugf("[%x] new cycle", hash[0:4])
							newCycle = true
						}()
					} else {
						poolLogger.Debugf("[%x] start cycle", hash[0:4])
						cycleStarted = true
					}
				}
				lastMissing = missing
				ready = true
				done = false
				// save a new processC (blocks still missing)
				offC = missingC
				missingC = processC
				// put processC offline
				processC = nil
				poolLogger.Debugf("[%x] ready for cycle %v", hash[0:4], blocksRequests)
			}
			//
			if ready && alarm {
				poolLogger.Debugf("[%x] check if new blocks arrived (attempt %v): missing %v/%v/%v", hash[0:4], blocksRequests, missing, total, depth)
				blocksRequestTimer = time.After(blocksRequestInterval * time.Second)
				i = 0
				missing = 0
				alarm = false
				ready = false
				// put back online
				processC = offC
			}
			select {
			case <-self.quit:
				break LOOP

			case <-self.flushC:
				flush = true
				alarm = true

			case waiter, ok := <-cycleC:
				self.cycleWG.Add(1)
				poolLogger.Debugf("[%x] cycle signal %v", hash[0:4], cycleN)
				cycle = true
				if ok {
					waiter <- true
					close(cycleC)
				}
				cycleC = nil
				alarm = true
				self.cycleLock.RLock()
				nextCycleC = self.cycleC
				nextCycleN = self.cycleN
				nextCycleWaiter = self.cycleWaiter
				self.cycleLock.RUnlock()

			case <-suicideTimer:
				self.killChain(node, nil)
				poolLogger.Warnf("[%x] timeout. (%v total attempts): missing %v/%v/%v", hash[0:4], blocksRequests, missing, total, depth)
				break LOOP

			case <-blocksRequestTimer:
				poolLogger.Debugf("[%x] block request time again", hash[0:4])
				alarm = true

			case <-blockHashesRequestTimer:
				poolLogger.Debugf("[%x] hash request time again", hash[0:4])

				if orignode != nil {
					orignode.RLock()
					parent := orignode.parent
					knownParent := orignode.knownParent
					orignode.RUnlock()
					if parent != nil && !knownParent {
						// if not root of chain, switch off
						poolLogger.Debugf("[%x] parent found, hash requests deactivated (after %v total attempts)\n", hash[0:4], blockHashesRequests)
						blockHashesRequestTimer = nil
					} else {
						blockHashesRequests++
						poolLogger.Debugf("[%x] hash request on root (%v total attempts)\n", hash[0:4], blockHashesRequests)
						self.requestBlockHashes(parent.hash)
						blockHashesRequestTimer = time.After(blockHashesRequestInterval * time.Second)
					}
				}
			case r, ok := <-controlC:
				if !ok {
					break LOOP
				}
				if running && !r {
					poolLogger.Debugf("[%x] idle mode", hash[0:4])
					if init {
						poolLogger.Debugf("[%x] off (%v total attempts): missing %v/%v/%v", hash[0:4], blocksRequests, missing, total, depth)
					}
					running = false
					alarm = false
					blocksRequestTimer = nil
					blockHashesRequestTimer = nil
					offC = processC
					processC = nil
					if cycle && cycleStarted {
						poolLogger.Debugf("[%x] finish cycle on going idle %v", hash[0:4], cycleN)
						self.cycleWG.Done()
						cycle = false
						cycleStarted = false
						go func() {
							<-cycleWaiter
							newCycle = true
						}()
					}
				}
				if !running && r {
					running = true
					poolLogger.Debugf("[%x] active mode", hash[0:4])
					node.sectionRLock()
					poolLogger.Debugf("[%x] check if complete", hash[0:4])
					complete := orignode.section.complete
					node.sectionRUnlock()
					if !complete {
						poolLogger.Debugf("[%x] activate block requests", hash[0:4])
						blocksRequestTimer = time.After(0)
					}
					orignode.RLock()
					parent := orignode.parent
					knownParent := orignode.knownParent
					orignode.RUnlock()

					if parent == nil && !knownParent {
						// if no parent but not connected to blockchain
						poolLogger.Debugf("[%x] activate block hashes requests", hash[0:4])
						blockHashesRequestTimer = time.After(0)
					} else {
						blockHashesRequestTimer = nil
					}
					if !init {
						// if not run at least once fully, launch iterator
						processC = make(chan *poolNode, blockHashesBatchSize)
						missingC = make(chan *poolNode, blockHashesBatchSize)
						self.foldUp(orignode, processC)
						i = 0
						total = 0
						lastMissing = 0
					} else {
						alarm = true
						processC = offC
					}
				}

			case <-resetC:
				init = false
				done = false
				missingC = nil
				processC = nil

			case node, ok := <-processC:
				if !ok && !init {
					// channel closed, first iteration finished
					init = true
					done = true
					processC = make(chan *poolNode, missing)

					total = missing
					depth = i
					if depth == 0 {
						break LOOP
					}
					poolLogger.Debugf("[%x] section initalised missing %v/%v/%v", hash[0:4], missing, total, depth)
					continue LOOP
				}
				poolLogger.Debugf("[%x] process node %v [%x]", hash[0:4], i, node.hash[0:4])
				i++
				// if node has no block
				node.RLock()
				block := node.block
				nhash := node.hash
				knownParent := node.knownParent
				node.RUnlock()
				if block == nil {
					poolLogger.Debugf("[%x] block missing on [%x]", hash[0:4], node.hash[0:4])
					missing++
					hashes = append(hashes, nhash)
					if len(hashes) == blockBatchSize {
						poolLogger.Debugf("[%x] request %v missing blocks", hash[0:4], len(hashes))
						self.requestBlocks(blocksRequests, hashes)
						hashes = nil
					}
					missingC <- node
				} else {
					// block is found
					if knownParent {
						// connected to the blockchain, insert the longest chain of blocks
						var blocks types.Blocks
						child := node
						parent := node
						node.sectionRLock()
						for child != nil && child.block != nil {
							parent = child
							blocks = append(blocks, parent.block)
							child = parent.child
						}
						node.sectionRUnlock()
						poolLogger.Debugf("[%x] insert %v blocks into blockchain", hash[0:4], len(blocks))
						if err := self.insertChain(blocks); err != nil {
							// TODO: not clear which peer we need to address
							// peerError should dispatch to peer if still connected and disconnect
							self.peerError(node.source, ErrInvalidBlock, "%v", err)
							poolLogger.Debugf("invalid block %v", node.hash)
							poolLogger.Debugf("penalise peers %v (hash), %v (block)", node.peer, node.source)
							// penalise peer in node.source
							self.killChain(node, nil)
							// self.disconnect()
							break LOOP
						}
						// pop the inserted ancestors off the channel
						for j := 1; j < len(blocks); j++ {
							i++
							<-processC
						}
						if child != nil {
							child.Lock()
							// mark the next one (no block yet) as connected to blockchain
							child.knownParent = true
							child.Unlock()
							// reset starting node to first node with missing block
							orignode = child
						}
						// delink inserted chain section
						self.killChain(node, parent)
					}
				}
				poolLogger.Debugf("[%x] %v/%v/%v/%v", hash[0:4], i, missing, total, depth)
				if i == lastMissing {
					poolLogger.Debugf("[%x] done", hash[0:4])
					done = true
				}
			} // select
		} // for
		poolLogger.Debugf("[%x] quit: %v block hashes requests - %v block requests - missing %v/%v/%v", hash[0:4], blockHashesRequests, blocksRequests, missing, total, depth)

		node.sectionLock()
		// this signals that controller not available
		node.section.controlC = nil
		node.sectionUnlock()
		self.wg.Done()
		if cycle {
			poolLogger.Debugf("[%x] finish cycle %v on quit", hash[0:4], cycleN)
			self.cycleWG.Done()
		}
	}()

}

func (self *BlockPool) peerError(peerId string, code int, format string, params ...interface{}) {
	self.peersLock.RLock()
	defer self.peersLock.RUnlock()
	peer, ok := self.peers[peerId]
	if ok {
		peer.peerError(code, format, params...)
	}
}

func (self *BlockPool) requestBlockHashes(hash []byte) {
	self.peersLock.Lock()
	defer self.peersLock.Unlock()
	if self.peer != nil {
		self.peer.requestBlockHashes(hash)
	}
}

func (self *BlockPool) requestBlocks(attempts int, hashes [][]byte) {
	// distribute block request among known peers
	self.peersLock.Lock()
	defer self.peersLock.Unlock()
	peerCount := len(self.peers)
	// on first attempt use the best peer
	if attempts == 0 {
		poolLogger.Debugf("request %v missing blocks from best peer %s", len(hashes), self.peer.id)
		self.peer.requestBlocks(hashes)
		return
	}
	repetitions := int(math.Min(float64(peerCount), float64(blocksRequestRepetition)))
	poolLogger.Debugf("request %v missing blocks from %v/%v peers", len(hashes), repetitions, peerCount)
	i := 0
	indexes := rand.Perm(peerCount)[0:repetitions]
	sort.Ints(indexes)
	for _, peer := range self.peers {
		if i == indexes[0] {
			peer.requestBlocks(hashes)
			indexes = indexes[1:]
			if len(indexes) == 0 {
				break
			}
		}
		i++
	}
}

func (self *BlockPool) getPeer(peerId string) (*peerInfo, bool) {
	self.peersLock.RLock()
	defer self.peersLock.RUnlock()
	if self.peer != nil && self.peer.id == peerId {
		return self.peer, true
	}
	info, ok := self.peers[peerId]
	if !ok {
		return nil, false
	}
	return info, false
}

func (self *peerInfo) addSection(hash []byte, section *section) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.sections[string(hash)] = section
}

func (self *peerInfo) addRoot(node *poolNode) {
	self.lock.Lock()
	defer self.lock.Unlock()
	poolLogger.Debugf("root node %x added to %s", node.hash[0:4], self.id)
	self.roots = append(self.roots, node)
}

// (re)starts processes registered for this peer (self)
func (self *peerInfo) start(peer *peerInfo) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.quitC = make(chan bool)
	poolLogger.Debugf("starting block hash requests starting from orphan blocks for %s", self.id)

	var roots []*poolNode
	for _, root := range self.roots {
		poolLogger.Debugf("check root node %x", root.hash[0:4])
		root.sectionRLock()
		if root.parent == nil {
			poolLogger.Debugf("orphan root node %x: request hashes", root.hash[0:4])
			self.requestBlockHashes(root.hash)
			roots = append(roots, root)
		} else {
			// if (other peers built on this), activate the chain
			// self.activateChain(root, self, true)
		}
		root.sectionRUnlock()
	}
	self.roots = roots
	poolLogger.Debugf("starting new processes for %s", self.id)
	self.controlSections(peer, true)
}

//  (re)starts process without requests, only suicide timer
func (self *peerInfo) stop(peer *peerInfo) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	close(self.quitC)
	poolLogger.Debugf("pausing block requests for %s", self.id)
	self.controlSections(peer, false)
}

func (self *peerInfo) controlSections(peer *peerInfo, on bool) {
	if peer != nil {
		peer.lock.RLock()
		defer peer.lock.RUnlock()
	}

	for hash, section := range self.sections {
		poolLogger.Debugf("section %x: on", hash[0:4])

		if section.done() {
			delete(self.sections, hash)
			continue
		}
		var found bool
		if peer != nil {
			poolLogger.Debugf("processes not found for %s", peer.id)
			_, found = peer.sections[hash]
		}

		poolLogger.Debugf("processes found for %s", self.id)
		// switch on processes not found in old peer
		// and switch off processes not found in new peer
		if !found {
			if on {
				// self is best peer
				poolLogger.Debugf("section %x: on", hash[0:4])
				section.start()
			} else {
				//  (re)starts process without requests, only suicide timer
				poolLogger.Debugf("section %x: off", hash[0:4])
				section.stop()
			}
		}
	}
}

// called when parent is found in pool
// parent and child are guaranteed to be on different sections
func (self *BlockPool) link(parent, child *poolNode) {
	var top bool
	parent.sectionLock()
	if child != nil {
		child.sectionLock()
	}
	if parent == parent.section.top && parent.section.top != nil {
		top = true
	}
	var bottom bool

	if child == child.section.bottom {
		bottom = true
	}
	if parent.child != child {
		orphan := parent.child
		if orphan != nil {
			// got a fork in the chain
			if top {
				orphan.lock.Lock()
				// make old child orphan
				orphan.parent = nil
				orphan.lock.Unlock()
			} else { // we are under section lock
				// make old child orphan
				orphan.parent = nil
				// reset section objects above the fork
				nchild := orphan.child
				node := orphan
				section := &section{bottom: orphan}
				for node.section == nchild.section {
					node = nchild
					node.section = section
					nchild = node.child
				}
				section.top = node
				// set up a suicide
				self.processSection(orphan)
				orphan.section.stop()
			}
		} else {
			// child is on top of a chain need to close section
			child.section.bottom = child
		}
		// adopt new child
		parent.child = child
		if !top {
			parent.section.top = parent
			// restart section process so that shorter section is scanned for blocks
			parent.section.reset()
		}
	}

	if child != nil {
		if child.parent != parent {
			stepParent := child.parent
			if stepParent != nil {
				if bottom {
					stepParent.Lock()
					stepParent.child = nil
					stepParent.Unlock()
				} else {
					// we are on the same section
					// if it is a aberrant reverse fork,
					stepParent.child = nil
					node := stepParent
					nparent := stepParent.child
					section := &section{top: stepParent}
					for node.section == nparent.section {
						node = nparent
						node.section = section
						node = node.parent
					}
				}
			} else {
				// linking to a root node, ie. parent is under the root of a chain
				parent.section.top = parent
			}
		}
		child.parent = parent
		child.section.bottom = child
	}
	// this needed if someone lied about the parent before
	child.knownParent = false

	parent.sectionUnlock()
	if child != nil {
		child.sectionUnlock()
	}
}

// this immediately kills the chain from node to end (inclusive) section by section
func (self *BlockPool) killChain(node *poolNode, end *poolNode) {
	self.wg.Add(1)
	go func() {
		self.lock.Lock()
		defer self.lock.Unlock()
		orignode := node
		orignode.sectionLock()
		defer orignode.sectionUnlock()
		poolLogger.Debugf("killing chain section [%x]-", node.hash[0:4])
		if end != nil {
			poolLogger.Debugf("[%x]", end.hash[0:4])
		}
		if orignode.section.controlC != nil {
			close(orignode.section.controlC)
			orignode.section.controlC = nil
		}
		delete(self.pool, string(node.hash))
		child := node.child
		top := node.section.top
		i := 1

		var quit bool
	LOOP:
		for node != top && node != end && child != nil {
			node = child
			select {
			case <-self.quit:
				quit = true
				break LOOP
			default:
			}
			poolLogger.Debugf("[%x] killing block\n", node.hash[0:4])
			delete(self.pool, string(node.hash))
			child = node.child
		}
		if !quit {
			if node == top {
				if node != end && child != nil && end != nil {
					//
					poolLogger.Debugf("[%x] calling kill on next section\n", child.hash[0:4])
					self.killChain(child, end)
				}
			} else {
				if child != nil {
					// delink rest of this section if ended midsection
					child.section.bottom = child
					child.parent = nil
				}
			}
		}
		orignode.section.bottom = nil
		poolLogger.Debugf("[%x] killed chain section (with %v blocks)\n", orignode.hash[0:4], i)
		self.wg.Done()
	}()
}

// structure to store long range links on chain to skip along
type section struct {
	lock     sync.RWMutex
	bottom   *poolNode
	top      *poolNode
	controlC chan bool
	resetC   chan bool
	complete bool
}

func (self *section) start() {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.controlC != nil {
		self.controlC <- true
	}
}

func (self *section) stop() {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.controlC != nil {
		self.controlC <- false
	}
}

func (self *section) reset() {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if self.controlC != nil {
		self.resetC <- true
		self.controlC <- false
	}
}

func (self *section) done() bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.controlC != nil {
		return true
	}
	return false
}

func (self *BlockPool) get(hash []byte) (node *poolNode) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.pool[string(hash)]
}

func (self *BlockPool) set(hash []byte, node *poolNode) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.pool[string(hash)] = node
}

// first time for block request, this iteration retrieves nodes of the chain
// from node up to top (all the way if nil) via child links
// copies the controller
// and feeds nodeC channel
// this is performed under section readlock to prevent top from going away
// when
func (self *BlockPool) foldUp(node *poolNode, nodeC chan *poolNode) {
	self.wg.Add(1)
	go func() {
		node.sectionRLock()
		defer node.sectionRUnlock()
	LOOP:
		for node != nil {
			select {
			case <-self.quit:
				break LOOP
			case nodeC <- node:
				if node == node.section.top {
					break LOOP
				}
				node = node.child
			}
		}
		close(nodeC)
		self.wg.Done()
	}()
}

func (self *poolNode) Lock() {
	self.sectionLock()
	self.lock.Lock()
}

func (self *poolNode) Unlock() {
	self.lock.Unlock()
	self.sectionUnlock()
}

func (self *poolNode) RLock() {
	self.lock.RLock()
}

func (self *poolNode) RUnlock() {
	self.lock.RUnlock()
}

func (self *poolNode) sectionLock() {
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.section.lock.Lock()
}

func (self *poolNode) sectionUnlock() {
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.section.lock.Unlock()
}

func (self *poolNode) sectionRLock() {
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.section.lock.RLock()
}

func (self *poolNode) sectionRUnlock() {
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.section.lock.RUnlock()
}
