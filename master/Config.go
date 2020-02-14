package master

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	ApiPort               int      `json:"apiPort"`
	ApiReadTimeout        int      `json:"apiReadTimeout"`
	ApiWriteTimeout       int      `json:"apiWriteTimeout"`
	EtcdEndPoints         []string `json:"etcdEndPoints"`
	EtcdDialTimeOut       int      `json:"etcdDialTimeout"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"`
}

var (
	G_config *Config
)

func InitConfig(filename string) error {

	var conf Config

	//fmt.Println("filename", filename)
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	err = json.Unmarshal(content, &conf)
	if err != nil {
		return err
	}
	G_config = &conf
	return nil
}
