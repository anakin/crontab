package main

import (
	"flag"
	"fmt"
	"github.com/anakin/crontab/worker"
	"runtime"
	"time"
)

var (
	configFile string
)

func initArgs() {

	flag.StringVar(&configFile, "config", "./worker.json", "use config file")
	flag.Parse()

}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
func main() {
	var (
		err error
	)
	initEnv()

	initArgs()
	if err = worker.InitConfig(configFile); err != nil {
		goto ERR
	}
	if err = worker.InitRegister(); err != nil {
		goto ERR
	}
	if err = worker.InitLogSink(); err != nil {
		goto ERR
	}
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}
	return
ERR:
	fmt.Println(err)
}
