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
	blocksRequestInterval      = 50 // ms
	blocksRequestRepetition    = 1
	blockHashesRequestInterval = 1000 // ms
	blocksRequestMaxIdleRounds = 100
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
	complete    bool
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
	purgeC  chan bool
	flushC  chan bool
	wg      sync.WaitGroup
	procWg  sync.WaitGroup
	running bool

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
	self.wg.Wait()

	self.peersLock.Lock()
	self.peers = nil
	self.peer = nil
	self.peersLock.Unlock()

	self.lock.Lock()
	self.pool = nil
	self.lock.Unlock()

	poolLogger.Infoln("Stopped")
}

func (self *BlockPool) Purge() {
	self.lock.Lock()
	if !self.running {
		self.lock.Unlock()
		return
	}
	self.lock.Unlock()

	poolLogger.Infoln("Purging...")
	close(self.purgeC)
	self.wg.Wait()

	self.purgeC = make(chan bool)

	poolLogger.Infoln("Stopped")

}

func (self *BlockPool) Wait(t time.Duration) {
	self.lock.Lock()
	if !self.running {
		self.lock.Unlock()
		return
	}
	self.lock.Unlock()

	poolLogger.Infoln("waiting for processes to complete...")
	close(self.flushC)
	w := make(chan bool)
	go func() {
		self.procWg.Wait()
		close(w)
	}()

	select {
	case <-w:
	case <-time.After(t):
		poolLogger.Debugf("completion timeout")
	}

	self.flushC = make(chan bool)

	poolLogger.Infoln("processes complete")

}

// AddPeer is called by the eth protocol instance running on the peer after
// the status message has been received with total difficulty and current block hash
// AddPeer can only be used once, RemovePeer needs to be called when the peer disconnects
func (self *BlockPool) AddPeer(td *big.Int, currentBlock []byte, peerId string, requestBlockHashes func([]byte) error, requestBlocks func([][]byte) error, peerError func(int, string, ...interface{})) bool {

	self.peersLock.Lock()
	defer self.peersLock.Unlock()
	peer, ok := self.peers[peerId]
	if ok {
		poolLogger.Debugf("update peer %v with td %v and current block %x", peerId, td, currentBlock[:4])
		peer.td = td
		peer.currentBlock = currentBlock
	} else {
		peer = &peerInfo{
			td:                 td,
			currentBlock:       currentBlock,
			id:                 peerId, //peer.Identity().Pubkey()
			requestBlockHashes: requestBlockHashes,
			requestBlocks:      requestBlocks,
			peerError:          peerError,
			sections:           make(map[string]*section),
		}
		self.peers[peerId] = peer
		poolLogger.Debugf("add new peer %v with td %v and current block %x", peerId, td, currentBlock[:4])
	}
	// check peer current head
	if self.hasBlock(currentBlock) {
		// peer not ahead
		return false
	}
	node := self.get(currentBlock)

	if node == nil {
		poolLogger.Debugf("create head block %x for peer %s", currentBlock[:4], peerId)

		// node created if not exist
		node = &poolNode{
			hash: currentBlock,
			peer: peerId,
			td:   td,
		}
		node.section = &section{
			top: node,
		}
		self.set(currentBlock, node)
	}
	peer.addRoot(node)
	if self.peer == peer {
		// new block update
		// peer is already active best peer, request hashes
		poolLogger.Debugf("peer %v already the best peer. request hashes from %x", peerId, currentBlock[:4])
		peer.requestBlockHashes(currentBlock)
		return true
	}

	currentTD := ethutil.Big0
	if self.peer != nil {
		currentTD = self.peer.td
	}
	if td.Cmp(currentTD) > 0 {
		poolLogger.Debugf("peer %v promoted to best peer", peerId)
		if self.peer != nil {
			self.peer.stop(peer)
		}
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

	self.wg.Add(1)
	self.procWg.Add(1)

	go func() {
		var child *poolNode
		var depth int
		var firstHash []byte
		var processSection bool

	LOOP:
		for {

			select {
			case <-self.quit:
				break LOOP
			case <-peer.quitC:
				// if the peer is demoted, no more hashes taken
				break LOOP
			default:
				// iterate using next (rlp stream lazy decoder) feeding hashesC
				hash, ok := next()
				if firstHash == nil {
					firstHash = hash
				}
				var knownParent bool
				if ok {
					poolLogger.Debugf("[%x] depth %v", hash[:4], depth)
					if self.hasBlock(hash) {
						// check if known block connecting the downloaded chain to our blockchain
						poolLogger.Debugf("[%x] known block", hash[0:4])
						// mark child as absolute pool root with parent known to blockchain
						knownParent = true
						ok = false
					}
				}
				if !ok {
					if child != nil {
						child.Lock()
						child.knownParent = knownParent
						child.section.bottom = child
						child.Unlock()
					}
					break LOOP
				}
				// look up node in pool
				parent := self.get(hash)
				if parent != nil {
					poolLogger.Debugf("[%x] found block", hash[:4])
					var fork bool
					// reached a known chain in the pool
					if child != nil {
						poolLogger.Debugf("[%x] reached blockpool chain", hash[:4])
						fork = self.link(parent, child)
					} else {
						// we expect the first block to be known, only continue if has no parent
						poolLogger.Debugf("[%x] first hash is known", hash[:4])
						parent.RLock()
						if parent.parent == nil {
							// the first block hash received is an orphan in the pool, so rejoice and continue
							poolLogger.Debugf("[%x] first hash is orphan block, keep building", hash[:4])
							processSection = true
							child = parent
							parent.RUnlock()
							continue LOOP
						}
						parent.RUnlock()
					}
					// activate the current chain
					poolLogger.Debugf("[%x] activate chain", hash[:4])
					self.activateChain(parent, peer, true, fork)
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
					depth++
				}
				if depth > 0 {
					parent.section = child.section
					poolLogger.Debugf("[%x] -> %x inherit section", hash[:4], child.hash[:4])
				} else {
					parent.section = &section{
						top: parent,
					}
					poolLogger.Debugf("[%x] -> %x new section", hash[:4], child.hash[:4])
				}
				child.parent = parent
				child.Unlock()
				self.set(hash, parent)
				poolLogger.Debugf("[%x] create node", hash[0:4])

				depth++
				child = parent

			} // select
		} //for
		self.wg.Done()
		self.procWg.Done()
		poolLogger.Debugf("[%x-%x] chain section of %v hashes added by peer %s", child.hash[:4], firstHash[:4], depth, peerId)
		if processSection {
			section := self.processSection(child)
			peer.addSection(child.hash, section)
			section.start()
		}
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
func (self *BlockPool) activateChain(node *poolNode, peer *peerInfo, on bool, reinit bool) {
	self.wg.Add(1)
	self.procWg.Add(1)
	poolLogger.Debugf("[%x] activate known chain for peer %s", node.hash[:4], peer.id)
	go func() {
		i := 0
	LOOP:
		for {
			node.sectionRLock()
			bottom := node.section.bottom
			if bottom == nil { // the chain section is being created or killed
				node.sectionRUnlock()
				break LOOP
			}
			poolLogger.Debugf("[%x-%x] activate", bottom.hash[:4], node.section.top.hash[:4])
			// register this section with the peer
			if peer != nil {
				peer.addSection(bottom.hash, bottom.section)
				if bottom.section.controlC != nil {
					poolLogger.Debugf("[%x-%x] restart section process", bottom.hash[:4], node.section.top.hash[:4])
					if i == 0 && reinit {
						// starting from mid section node
						poolLogger.Debugf("[%x-%x] reinitialise section", bottom.hash[:4], node.section.top.hash[:4])
						bottom.section.controlC <- false
						bottom.section.resetC <- true
					}
					bottom.section.controlC <- on
					poolLogger.Debugf("[%x-%x] start section process - active %v", bottom.hash[:4], node.section.top.hash[:4], on)
				} else {
					poolLogger.Debugf("[%x-%x] found section process complete", bottom.hash[:4], node.section.top.hash[:4])
				}
				// if complete no need to reset
			}
			i++
			select {
			case <-peer.quitC:
				bottom.sectionRUnlock()
				break LOOP
			case <-self.quit:
				bottom.sectionRUnlock()
				break LOOP
			default:
			}
			node = bottom.parent
			bottom.sectionRUnlock()
			if node == nil {
				node = bottom
				break LOOP
			}
		}
		// remember root for this peer if on == true
		if on && peer != nil {
			poolLogger.Debugf("[%x] add orphan", node.hash[0:4])
			peer.requestBlockHashes(node.hash)
			peer.addRoot(node)
		}
		self.wg.Done()
		self.procWg.Done()
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
func (self *BlockPool) processSection(node *poolNode) (section *section) {

	node.Lock()
	defer node.Unlock()
	section = node.section
	if section.controlC != nil {
		return
	}
	controlC := make(chan bool, 1)
	resetC := make(chan bool, 1)
	section.controlC = controlC
	section.resetC = resetC

	self.wg.Add(1)
	go func() {
		// absolute time after which sub-chain is killed if not complete (some blocks are missing)
		suicideTimer := time.After(blockTimeout * time.Minute)

		var blocksRequestTimer, blockHashesRequestTimer <-chan time.Time
		var blocksRequestTime, blockHashesRequestTime bool
		var blocksRequests, blockHashesRequests int
		var blocksRequestsComplete, blockHashesRequestsComplete bool

		// node channels for the section
		var missingC, processC, offC chan *poolNode
		// container for missing block hashes
		var hashes [][]byte

		var i, total, missing, lastMissing, depth int
		var idle int
		var init, done, same, running, ready bool

		hash := node.hash[:4]
		poolLogger.Debugf("[%x] start section process", hash)

		blockHashesRequestRoot := node

	LOOP:
		for {
			if blockHashesRequestsComplete && blocksRequestsComplete {
				// not waiting for hashes any more
				poolLogger.Debugf("[%x] section complete %v blocks retrieved (%v attempts), hash requests complete on %x (%v attempts)", hash, depth, blocksRequests, blockHashesRequestRoot.hash[:4], blockHashesRequests)
				break LOOP
			} // otherwise suicide if no hashes coming
			if done {
				// went through all blocks in section
				if missing == 0 {
					// no missing blocks
					poolLogger.Debugf("[%x] got all blocks. process complete (%v total blocksRequests): missing %v/%v/%v", hash, blocksRequests, missing, total, depth)
					node.sectionLock()
					node.section.complete = true
					node.sectionUnlock()
					blocksRequestsComplete = true
					blocksRequestTimer = nil
				} else {
					// some missing blocks
					blocksRequests++
					poolLogger.Debugf("[%x] block request attempt %v: missing %v/%v/%v", hash, blocksRequests, missing, total, depth)
					if len(hashes) > 0 {
						// send block requests to peers
						self.requestBlocks(blocksRequests, hashes)
						hashes = nil
					}
					poolLogger.Debugf("[%x] check if there is missing blocks", hash)
					if missing == lastMissing {
						// idle round
						if same {
							// more than once
							idle++
							// too many idle rounds
							if idle >= blocksRequestMaxIdleRounds {
								poolLogger.Debugf("[%x] block requests had %v idle rounds (%v total attempts): missing %v/%v/%v\ngiving up...", hash, idle, blocksRequests, missing, total, depth)
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
				poolLogger.Debugf("[%x] done checking missing blocks", hash)
				lastMissing = missing
				ready = true
				done = false
				// save a new processC (blocks still missing)
				offC = missingC
				missingC = processC
				// put processC offline
				processC = nil
				poolLogger.Debugf("[%x] ready for cycle %v", hash, blocksRequests)
			}
			//
			if ready && blocksRequestTime {
				poolLogger.Debugf("[%x] check if new blocks arrived (attempt %v): missing %v/%v/%v", hash, blocksRequests, missing, total, depth)
				blocksRequestTimer = time.After(blocksRequestInterval * time.Millisecond)
				blocksRequestTime = false
				processC = offC
			}
			if blockHashesRequestTime {
				blockHashesRequestRoot.RLock()
				parent := blockHashesRequestRoot.parent
				knownParent := blockHashesRequestRoot.knownParent
				blockHashesRequestRoot.RUnlock()
				if parent != nil || knownParent {
					// if not root of chain, switch off
					poolLogger.Debugf("[%x] parent found, hash requests deactivated (after %v total attempts)\n", hash, blockHashesRequests)
					blockHashesRequestTimer = nil
					blockHashesRequestsComplete = true
				} else {
					blockHashesRequests++
					poolLogger.Debugf("[%x] hash request on root (%v total attempts)\n", hash, blockHashesRequests)
					self.requestBlockHashes(blockHashesRequestRoot.hash)
					blockHashesRequestTimer = time.After(blockHashesRequestInterval * time.Millisecond)
				}
				blockHashesRequestTime = false
			}

			poolLogger.Debugf("[%x] select", hash)
			select {

			case <-self.quit:
				break LOOP

			case <-self.purgeC:
				suicideTimer = time.After(0)

			case <-suicideTimer:
				self.killChain(node, nil)
				poolLogger.Debugf("[%x] timeout. (%v total attempts): missing %v/%v/%v", hash, blocksRequests, missing, total, depth)
				break LOOP

			case <-blocksRequestTimer:
				poolLogger.Debugf("[%x] block request time again", hash)
				blocksRequestTime = true

			case <-blockHashesRequestTimer:
				poolLogger.Debugf("[%x] hash request time again", hash)
				blockHashesRequestTime = true

			case r, ok := <-controlC:
				if !ok {
					break LOOP
				}
				if running && !r {
					self.procWg.Done()
					poolLogger.Debugf("[%x] idle mode", hash)
					if init {
						poolLogger.Debugf("[%x] off (%v total attempts): missing %v/%v/%v", hash, blocksRequests, missing, total, depth)
					}

					running = false
					blocksRequestTime = false
					blocksRequestTimer = nil
					blockHashesRequestTime = false
					blockHashesRequestTimer = nil
					if processC != nil {
						offC = processC
						processC = nil
					}
					poolLogger.Debugf("[%x] idle mode on", hash)
				}
				if !running && r {
					running = true
					self.procWg.Add(1)

					poolLogger.Debugf("[%x] active mode", hash)
					poolLogger.Debugf("[%x] check if complete", hash)
					if !blocksRequestsComplete {
						poolLogger.Debugf("[%x] activate block requests", hash)
						blocksRequestTime = true
					}
					if !blockHashesRequestsComplete {
						poolLogger.Debugf("[%x] activate block hashes requests", hash)
						blockHashesRequestTime = true
					}
					if !init {
						// if not run at least once fully, launch iterator
						processC = make(chan *poolNode, blockHashesBatchSize)
						missingC = make(chan *poolNode, blockHashesBatchSize)
						poolLogger.Debugf("[%x] initialise section", hash)
						self.foldUp(blockHashesRequestRoot, processC)
						i = 0
						missing = 0
						total = 0
						lastMissing = 0
						depth = 0
					} else {
						processC = offC
					}
				}

			case <-resetC:
				poolLogger.Debugf("[%x] reinit section", hash)
				init = false
				done = false
				ready = false

			case node, ok := <-processC:
				if !ok && !init {
					// channel closed, first iteration finished
					init = true
					done = true
					processC = make(chan *poolNode, missing)

					total = missing
					depth = i
					if depth == 0 {
						poolLogger.Debugf("[%x] empty section", hash)
						// self.killChain(node, end)
						break LOOP
					}
					poolLogger.Debugf("[%x] section initalised: missing %v/%v/%v", hash, missing, total, depth)
					continue LOOP
				}
				if ready {
					i = 0
					missing = 0
					ready = false
				}
				poolLogger.Debugf("[%x] process node %v [%x]", hash, i, node.hash[:4])
				i++
				// if node has no block
				node.RLock()
				block := node.block
				nhash := node.hash
				knownParent := node.knownParent
				node.RUnlock()
				if block == nil {
					poolLogger.Debugf("[%x] block missing on [%x]", hash, node.hash[:4])
					missing++
					hashes = append(hashes, nhash)
					if len(hashes) == blockBatchSize {
						poolLogger.Debugf("[%x] request %v missing blocks", hash, len(hashes))
						self.requestBlocks(blocksRequests, hashes)
						hashes = nil
					}
					missingC <- node
				} else {
					// block is found
					node.Lock()
					// node is marked as complete so that it can be inserted in the block chain
					// if only node.block != nil was checked, the node could still be in the missingC channel
					node.complete = true
					node.Unlock()
					if knownParent {
						// connected to the blockchain, insert the longest chain of blocks
						poolLogger.Debugf("[%x] reached blockchain", hash)
						n, err := self.addChain(node)
						if err != nil {
							break LOOP
						}
						poolLogger.Debugf("[%x] added %v blocks", hash, n)
						// pop the inserted ancestors off the channel
						for j := 1; j < n; j++ {
							select {
							case <-processC:
								i++
							default:
								// if chain of complete nodes went to next section up
								// then len(blocks) > number of nodes in the channel
								break
							}
						}
					}
				}
				poolLogger.Debugf("[%x] %v/%v/%v/%v", hash, i, missing, total, depth)
				if i == lastMissing {
					poolLogger.Debugf("[%x] done", hash)
					done = true
				}
			} // select
		} // for
		poolLogger.Debugf("[%x] quit: %v block hashes requests - %v block requests - missing %v/%v/%v", hash, blockHashesRequests, blocksRequests, missing, total, depth)

		node.sectionLock()
		// this signals that controller not available
		poolLogger.Debugf("[%x] process complete", hash)
		node.section.controlC = nil
		node.sectionUnlock()

		self.wg.Done()
		if running {
			self.procWg.Done()
		}
		poolLogger.Debugf("[%x] process complete", hash)
	}()
	return
}

func (self *BlockPool) addChain(node *poolNode) (n int, err error) {
	var blocks types.Blocks
	parent := node
	child := node
	// iterate up along complete nodes potentially across multiple sections
	for child != nil {
		child.Lock()
		if child.section == node.section && child.block != nil {
			child.complete = true
		}
		if !child.complete {
			// mark the next one (no block yet) as connected to blockchain
			poolLogger.Debugf("[%x] marking known parent ", child.hash[:4])
			child.knownParent = true
			if node.section != child.section {
				poolLogger.Debugf("[%x] restart idle section", child.hash[:4])
				child.section.controlC <- true
			}
			child.Unlock()
			break
		}
		blocks = append(blocks, child.block)
		parent = child
		child = parent.child
		parent.Unlock()
	}
	poolLogger.Debugf("insert %v blocks into blockchain", len(blocks))
	err = self.insertChain(blocks)
	if err != nil {
		// TODO: not clear which peer we need to address
		// peerError should dispatch to peer if still connected and disconnect
		self.peerError(node.source, ErrInvalidBlock, "%v", err)
		poolLogger.Debugf("invalid block %x", node.hash)
		poolLogger.Debugf("penalise peers %v (hash), %v (block)", node.peer, node.source)
		// penalise peer in node.source
		self.killChain(node, nil)
		// self.disconnect()

	} else {
		// delink inserted chain section
		self.killChain(node, parent)
	}
	n = len(blocks)
	return
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
		poolLogger.Debugf("request hashes starting on %x from best peer %s", hash[:4], self.peer.id)
		self.peer.requestBlockHashes(hash)
	}
}

func (self *BlockPool) requestBlocks(attempts int, hashes [][]byte) {
	// distribute block request among known peers
	poolLogger.Debugf("request blocks")
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
	i := 0
	indexes := rand.Perm(peerCount)[0:repetitions]
	sort.Ints(indexes)
	poolLogger.Debugf("request %v missing blocks from %v/%v peers: chosen %v", len(hashes), repetitions, peerCount, indexes)
	for _, peer := range self.peers {
		if i == indexes[0] {
			poolLogger.Debugf("request %v missing blocks from %s", len(hashes), peer.id)
			peer.requestBlocks(hashes)
			indexes = indexes[1:]
			if len(indexes) == 0 {
				break
			}
		}
		i++
	}
	poolLogger.Debugf("done requesting blocks")

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
	poolLogger.Debugf("section process %x added to %s", hash[:4], self.id)
	self.sections[string(hash)] = section
}

func (self *peerInfo) addRoot(node *poolNode) {
	self.lock.Lock()
	defer self.lock.Unlock()
	poolLogger.Debugf("[%s] add orphan %x", self.id, node.hash[:4])
	self.roots = append(self.roots, node)
}

// (re)starts processes registered for this peer (self)
func (self *peerInfo) start(peer *peerInfo) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.quitC = make(chan bool)
	poolLogger.Debugf("[%s] block hash requests starting from orphan blocks", self.id)

	var roots []*poolNode
	for _, root := range self.roots {
		poolLogger.Debugf("[%s] check orphan %x", self.id, root.hash[:4])
		root.sectionRLock()
		if root.parent == nil {
			poolLogger.Debugf("[%s] found orphan %x", self.id, root.hash[:4])
			self.requestBlockHashes(root.hash)
			roots = append(roots, root)
		} else {
			// if (other peers built on this), activate the chain
			// self.activateChain(root, self, true)
		}
		root.sectionRUnlock()
	}
	self.roots = roots
	poolLogger.Debugf("[%s] activate section processes", self.id)
	self.controlSections(peer, true)
}

//  (re)starts process without requests, only suicide timer
func (self *peerInfo) stop(peer *peerInfo) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	close(self.quitC)
	poolLogger.Debugf("[%s] inactivate section processes", self.id)
	self.controlSections(peer, false)
}

func (self *peerInfo) controlSections(peer *peerInfo, on bool) {
	if peer != nil {
		peer.lock.RLock()
		defer peer.lock.RUnlock()
	}

	for hash, section := range self.sections {

		if section.done() {
			poolLogger.Debugf("[%s][%x] section process complete - remove", self.id, hash[:4])
			delete(self.sections, hash)
			continue
		}
		var found bool
		if peer != nil {
			_, found = peer.sections[hash]
		}

		// switch on processes not found in old peer
		// and switch off processes not found in new peer
		if !found {
			if on {
				// self is best peer
				poolLogger.Debugf("[%s][%x] section process -> active", self.id, hash[:4])
				section.start()
			} else {
				//  (re)starts process without requests, only suicide timer
				poolLogger.Debugf("[%s][%x] section process -> inactive", self.id, hash[:4])
				section.stop()
			}
		}
	}
}

// called when parent is found in pool
// parent and child are guaranteed to be on different sections
func (self *BlockPool) link(parent, child *poolNode) (fork bool) {
	var top bool
	parent.sectionLock()
	if child != nil {
		child.sectionLock()
	}
	if parent == parent.section.top && parent.section.top != nil {
		top = true
	}
	poolLogger.Debugf("link %x - %x", parent.hash[:4], child.hash[:4])
	var bottom bool

	if child == child.section.bottom {
		bottom = true
	}
	if parent.child != child {
		orphan := parent.child
		if orphan != nil {
			// got a fork in the chain
			poolLogger.Debugf("FORK %x -> %x/%x", parent.hash[:4], child.hash[:4], orphan.hash[:4])
			if top {
				orphan.lock.Lock()
				// make old child orphan
				orphan.parent = nil
				poolLogger.Debugf("inactivate section %x-%x", orphan.hash[:4], orphan.section.top.hash[:4])
				orphan.section.stop()
				orphan.lock.Unlock()
			} else { // we are under section lock
				// make old child orphan
				fork = true
				poolLogger.Debugf("parent is mid section %x", parent.hash[:4])
				orphan.parent = nil
				// reset section objects above the fork
				node := orphan
				topnode := node
				section := &section{
					bottom: orphan,
				}
				orphansection := orphan.section
				for node != nil && node.section == orphansection {
					poolLogger.Debugf("-> new section for %x (%x-%x)", node.hash[:4], node.section.bottom.hash[:4], node.section.top.hash[:4])
					node.section = section
					topnode = node
					node = node.child
				}
				section.top = topnode
				// set up a suicide
				poolLogger.Debugf("set up suicide for new section %x-%x", orphan.hash[:4], topnode.hash[:4])
				self.processSection(orphan).stop()
			}
		} else {
			// child is on top of a chain need to close section
			poolLogger.Debugf("parent is head of section %x", parent.hash[:4])
			child.section.bottom = child
		}
		// adopt new child
		parent.child = child
		if !top {
			parent.section.top = parent
			poolLogger.Debugf("parent is mid section %x (%x-%x): split section", parent.hash[:4], parent.section.bottom.hash[:4], parent.section.top.hash[:4])
			// restart section process so that shorter section is scanned for blocks
		}
	}

	if child != nil {
		poolLogger.Debugf("link child %x", child.hash[:4])
		stepParent := child.parent
		if stepParent != nil && stepParent != parent {
			poolLogger.Debugf("reverse fork (two different parents!!)")
			if !bottom {
				poolLogger.Debugf("section boundary %x-%x", parent.hash[:4], child.hash[:4])
				stepParent.Lock()
				stepParent.child = nil
				stepParent.Unlock()
			} else {
				// step parent is in the same section as child,
				// need to split, no need to lock
				stepParent.child = nil
				node := stepParent
				section := &section{
					top: stepParent,
				}
				for node != nil && node.section == stepParent.section {
					node.section = section
					node = node.parent
				}
			} // step parent is head of a section, will clean up
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
	poolLogger.Debugf("link %x - %x done", parent.hash[:4], child.hash[:4])
	return
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
				// only kill section process if whole section killed
				if node.section.controlC != nil {
					close(node.section.controlC)
					poolLogger.Debugf("[%x] killing block\n", node.hash[0:4])
					node.section.controlC = nil
				}
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

func (self *section) done() bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.controlC == nil {
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
