package stay

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func generateBlockName() string {
	need := time.Now()
	return fmt.Sprintf("%s-%d.log", need.Format("20060102-150405"), need.Unix())
}

func dirExist(dirname string) bool {
	fi, err := os.Stat(dirname)
	return (err == nil || os.IsExist(err)) && fi.IsDir()
}

type Block struct {
	sync.Mutex
	Name    string
	Size    int64
	rawPath string
	fd      *os.File
	writer  *bufio.Writer
}

func NewBlock(file string) (*Block, error) {

	blockPath, err := filepath.Abs(file)
	if err != nil {
		return nil, err
	}

	blockPath = filepath.Dir(blockPath)
	if ret := dirExist(blockPath); !ret {
		if err := os.MkdirAll(blockPath, 0755); err != nil {
			return nil, err
		}
	}

	fd, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}

	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	return &Block{
		rawPath: file,
		Name:    fi.Name(),
		Size:    fi.Size(),
		fd:      fd,
		writer:  bufio.NewWriter(fd),
	}, nil
}

func (block *Block) Write(data []byte) error {

	block.Lock()
	defer block.Unlock()
	if block.writer == nil {
		return fmt.Errorf("block writer invalid")
	}

	_, err := fmt.Fprintln(block.writer, string(data))
	if err != nil {
		return err
	}

	block.writer.Flush()
	block.Size += (int64)(len(data))
	return nil
}

func (block *Block) Read() ([]byte, error) {

	block.Lock()
	defer block.Unlock()
	if block.fd == nil {
		return nil, fmt.Errorf("block fd invalid")
	}

	size := block.Size
	_, err := block.fd.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(block.fd)
	block.fd.Seek(size, os.SEEK_CUR)
	return data, err
}

func (block *Block) Close(remove bool) error {

	block.Lock()
	defer func() {
		if remove {
			os.Remove(block.rawPath)
		}
		block.Unlock()
	}()

	if block.fd != nil {
		if err := block.fd.Close(); err != nil {
			return err
		}
	}
	return nil
}
