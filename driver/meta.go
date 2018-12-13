package driver

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/docker/docker/daemon/logger"
)

const metaRootPath string = "/run/docker/logging/meta_confs"

type MetaInfo struct {
	File string
	Info *logger.Info
}

func loadMeta() map[string]*MetaInfo {

	metaInfos := map[string]*MetaInfo{}
	fis, err := ioutil.ReadDir(metaRootPath)
	if err == nil {
		for _, fic := range fis {
			if !fic.IsDir() {
				file := fic.Name()
				if metaInfo, err := readMeta(file); err == nil {
					metaInfos[file] = metaInfo
				}
			}
		}
	}
	return metaInfos
}

func readMeta(file string) (*MetaInfo, error) {

	metaFile := filepath.Join(metaRootPath, path.Base(file))
	data, err := ioutil.ReadFile(metaFile)
	if err != nil {
		return nil, err
	}

	var metaInfo MetaInfo
	if err = json.Unmarshal(data, &metaInfo); err != nil {
		return nil, err
	}
	return &metaInfo, nil
}

func writeMeta(file string, info *logger.Info) error {

	metaInfo := MetaInfo{
		File: file,
		Info: info,
	}

	if err := os.MkdirAll(metaRootPath, 0755); err != nil {
		return err
	}

	data, err := json.Marshal(&metaInfo)
	if err != nil {
		return err
	}

	metaFile := filepath.Join(metaRootPath, path.Base(file))
	return ioutil.WriteFile(metaFile, data, 0755)
}

func removeMeta(file string) error {

	metaFile := filepath.Join(metaRootPath, path.Base(file))
	return os.Remove(metaFile)
}
