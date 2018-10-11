package main

import (
	"os"

	"github.com/docker/go-plugins-helpers/sdk"
	hblogs "github.com/humpback/gounits/logger"
	"github.com/humpback/gounits/system"
	"github.com/humpback/humpback-logdriver/conf"
	"github.com/humpback/humpback-logdriver/driver"
	"github.com/humpback/humpback-logdriver/handlers"
)

func main() {

	if err := conf.New("./conf/config.yaml"); err != nil {
		panic(err)
	}

	hblogs.OPEN(conf.LoggerArgs())
	hblogs.INFO("[#main#] plugin driver environment is [%s]", conf.Environment())
	pluginDriver, err := driver.New()
	if err != nil {
		panic(err)
	}

	defer func() {
		pluginDriver.Close()
		hblogs.INFO("[#main#] plugin driver exited.")
		hblogs.CLOSE()
		os.Exit(0)
	}()

	go func() {
		handler := sdk.NewHandler(`{"Implements": ["LoggingDriver"]}`)
		handlers.Routes(&handler, pluginDriver)
		err := handler.ServeUnix("humpbacklogdriver", 0)
		if err != nil {
			panic(err)
		}
	}()
	system.InitSignal(nil)
}
