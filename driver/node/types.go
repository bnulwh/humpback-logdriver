package node

var PluginPath = "humpback/plugins/logdriver"

type WatchProvidersHandleFunc func(data []byte)

type NodeData struct {
	Plugin      string      `json:"plugin"`
	Environment string      `json:"environment"`
	IPAddr      string      `json:"ipaddr"`
	HostName    string      `json:"hostname"`
	Provider    interface{} `json:"provider"`
	Timestamp   int64       `json:"timestamp"`
}
