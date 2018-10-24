package master

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	ApiPort         int `json:"apiPort"`
	ApiReadTimeout  int `json:"apiReadTimeout"`
	ApiWriteTimeout int `json:"apiWriteTimeout"`
}

var (
	G_config *Config
)

func InitConfig(filename string) (err error) {

	var (
		content []byte
		conf    Config
	)
	//fmt.Println("filename", filename)
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	G_config = &conf
	//fmt.Println(conf)
	return
}
