package node

import (
	"os"
	"time"

	"github.com/humpback/discovery"
	"github.com/humpback/gounits/json"
	hblogs "github.com/humpback/gounits/logger"
	"github.com/humpback/gounits/network"
	"github.com/humpback/humpback-logdriver/conf"
)

type Node struct {
	Config    *conf.NodeConfig
	Data      *NodeData
	Discovery *discovery.Discovery
	stopCh    chan struct{}
}

func New(pluginName string, nodeConfig *conf.NodeConfig) (*Node, error) {

	configOpts := map[string]string{"kv.path": PluginPath}
	discovery, err := discovery.New(nodeConfig.Hosts, nodeConfig.Heartbeat, nodeConfig.TTL, configOpts)
	if err != nil {
		return nil, err
	}

	hostName, _ := os.Hostname()
	nodeData := &NodeData{
		Plugin:      pluginName,
		Environment: nodeConfig.Environment,
		IPAddr:      network.GetDefaultIP(),
		HostName:    hostName,
		Provider:    nil,
		Timestamp:   time.Now().Unix(),
	}

	return &Node{
		Config:    nodeConfig,
		Data:      nodeData,
		Discovery: discovery,
		stopCh:    make(chan struct{}),
	}, nil
}

func (pnode *Node) Register() error {

	buf, err := json.EnCodeObjectToBuffer(pnode.Data)
	if err != nil {
		return err
	}

	hblogs.INFO("[#node#] plugin node register to %s discovery - [addr:%s]", pnode.Config.Environment, pnode.Data.IPAddr)
	pnode.Discovery.Register(pnode.Data.IPAddr, buf, pnode.stopCh, func(key string, err error) {
		if err != nil {
			hblogs.ERROR("[#node#] plugin node register %s error, %s", key, err.Error())
		}
	})
	return nil
}

func (pnode *Node) Close() error {

	close(pnode.stopCh)
	hblogs.INFO("[#node#] plugin node close.")
	return nil
}

func (pnode *Node) WatchProviders(fn WatchProvidersHandleFunc) {

	pnode.Discovery.WatchExtend(PluginPath+"/providers", pnode.stopCh,
		func(key string, data []byte, err error) {
			if err != nil {
				hblogs.ERROR("[#node#] plugin node watch provides %s %s error, %s", pnode.Config.Environment, key, err)
				return
			}
			fn(data)
		})
}
