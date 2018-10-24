package main

import (
	"flag"
	"fmt"
	"github.com/anakin/crontab/master"
	"runtime"
)

var (
	configFile string
)

func initArgs() {

	flag.StringVar(&configFile, "config", "./master.json", "use config file")
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
	if err = master.InitConfig(configFile); err != nil {
		goto ERR
	}

	if err = master.InitApiServer(); err != nil {
		goto ERR
	}
	return
ERR:
	fmt.Println(err)
}