package handlers

import "github.com/docker/docker/daemon/logger"

type StartLoggingRequest struct {
	File string
	Info logger.Info
}

type StopLoggingRequest struct {
	File string
}

type ReadLogsRequest struct {
	Info   logger.Info
	Config logger.ReadConfig
}
