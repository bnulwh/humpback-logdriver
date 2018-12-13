package stay

import (
	"fmt"
	"io/ioutil"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	rootPath             = "/run/docker/logging/blocks"
	defaultMaxSize       = (int64)(3 * 1024 << 10) //3M size
	defaultMaxCount      = 40
	defaultRetryInterval = 180 * time.Second
)

var (
	useConfig = &StayBlocksConfig{
		Enable:        true,
		MaxSize:       defaultMaxSize,
		MaxCount:      defaultMaxCount,
		RetryInterval: defaultRetryInterval,
	}
)

type BlockHandleFunc func(block string, data []byte) error

type blockInfo struct {
	Name      string
	Timestemp int64
	Size      int64
}

type StayBlocksConfig struct {
	Enable        bool
	MaxSize       int64
	MaxCount      int
	RetryInterval time.Duration
}

type StayBlocks struct {
	sync.Mutex
	currBlock *Block
	fn        BlockHandleFunc
	stopCh    chan struct{}
}

func NewStayBlocks(config *StayBlocksConfig, fn BlockHandleFunc) *StayBlocks {

	if config != nil {
		useConfig = config
	}

	if useConfig.MaxSize <= 0 {
		useConfig.MaxSize = defaultMaxSize
	}

	if useConfig.MaxCount < 0 {
		useConfig.MaxCount = defaultMaxCount
	}

	if useConfig.RetryInterval == time.Duration(0) {
		useConfig.RetryInterval = defaultRetryInterval
	}

	block := readBlock()
	blocks := &StayBlocks{
		currBlock: block,
		fn:        fn,
		stopCh:    make(chan struct{}),
	}

	if useConfig.Enable {
		go blocks.runLoop()
	}
	return blocks
}

func (blocks *StayBlocks) Write(data []byte) error {

	if !useConfig.Enable {
		return fmt.Errorf("blocks no enable")
	}

	blocks.Lock()
	defer blocks.Unlock()
	if blocks.currBlock != nil && blocks.currBlock.Size < useConfig.MaxSize {
		return blocks.currBlock.Write(data)
	}

	swapBlock, err := writeBlock(data)
	if err != nil {
		return err
	}

	if swapBlock {
		if blocks.currBlock != nil {
			blocks.currBlock.Close(true)
		}
		blocks.currBlock = readBlock()
	}
	return nil
}

func (blocks *StayBlocks) Close() {

	blocks.Lock()
	if blocks.currBlock != nil {
		blocks.currBlock.Close(false)
		blocks.currBlock = nil
	}
	blocks.Unlock()
	close(blocks.stopCh)
}

func (blocks *StayBlocks) runLoop() {

	originalDuraion := useConfig.RetryInterval
	for {
		ticker := time.NewTicker(useConfig.RetryInterval)
		select {
		case <-ticker.C:
			{
				ticker.Stop()
				blocks.Lock()
				if blocks.currBlock != nil {
					data, err := blocks.currBlock.Read()
					if err != nil {
						blocks.currBlock.Close(true)
						blocks.currBlock = readBlock()
						blocks.Unlock()
						break
					}
					if blocks.fn != nil {
						err = blocks.fn(blocks.currBlock.Name, data)
						if err == nil {
							blocks.currBlock.Close(true)
							blocks.currBlock = readBlock()
							useConfig.RetryInterval, _ = time.ParseDuration("10s")
						} else {
							useConfig.RetryInterval = originalDuraion
						}
					}
				}
				blocks.Unlock()
			}
		case <-blocks.stopCh:
			{
				ticker.Stop()
				return
			}
		}
	}
}

func writeBlock(data []byte) (bool, error) {

	var (
		bsInfo     []*blockInfo
		blockName  string
		blockCount int
		bSwapBlock bool
	)

	bsInfo = blocksInfo()
	less := func(i, j int) bool {
		if bsInfo[i].Timestemp != bsInfo[j].Timestemp {
			return bsInfo[i].Timestemp > bsInfo[j].Timestemp
		} else {
			ret := strings.Compare(bsInfo[i].Name, bsInfo[j].Name)
			if ret > 0 {
				return true
			}
			return false
		}
	}
	sort.Slice(bsInfo, less)
	blockCount = len(bsInfo)
	if blockCount == 0 {
		bSwapBlock = true
		blockName = generateBlockName()
	} else {
		if bsInfo[0].Size < useConfig.MaxSize {
			blockName = bsInfo[0].Name
		} else {
			blockName = generateBlockName()
			if useConfig.MaxCount > 0 && blockCount >= useConfig.MaxCount {
				bSwapBlock = true
			}
		}
	}

	pBlock, err := NewBlock(path.Join(rootPath, blockName))
	if err != nil {
		return false, err
	}

	defer func() {
		if !bSwapBlock {
			pBlock.Close(false)
		}
	}()

	err = pBlock.Write(data)
	return bSwapBlock, err
}

func readBlock() *Block {

	bsInfo := blocksInfo()
	if len(bsInfo) == 0 {
		return nil
	}

	less := func(i, j int) bool {
		if bsInfo[i].Timestemp != bsInfo[j].Timestemp {
			return bsInfo[i].Timestemp < bsInfo[j].Timestemp
		} else {
			ret := strings.Compare(bsInfo[i].Name, bsInfo[j].Name)
			if ret < 0 {
				return true
			}
			return false
		}
	}

	sort.Slice(bsInfo, less)
	bInfo := bsInfo[0]
	block, err := NewBlock(path.Join(rootPath, bInfo.Name))
	if err != nil {
		return nil
	}
	return block
}

func blocksInfo() []*blockInfo {

	fis, err := ioutil.ReadDir(rootPath)
	if err != nil {
		return []*blockInfo{}
	}

	var (
		bsInfo = []*blockInfo{}
		nRead  = 0
	)

	for _, fic := range fis {
		if !fic.IsDir() {
			if useConfig.MaxCount > 0 && nRead >= useConfig.MaxCount {
				break
			}
			bsInfo = append(bsInfo, &blockInfo{
				Name:      fic.Name(),
				Timestemp: fic.ModTime().UnixNano(),
				Size:      fic.Size(),
			})
			nRead++
		}
	}
	return bsInfo
}
