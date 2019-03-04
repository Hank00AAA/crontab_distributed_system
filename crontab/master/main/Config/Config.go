package Config

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct{
	ApiPort int `json:"apiPort"`
	ApiReadTimeout int `json:"apiReadTimeout"`
	ApiWriteTimeout int `json:"apiWriteTimeout"`
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	WebRoot string `json:"webroot"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectionTimeout int `json:"mongodbConnectionTimeout"`
}

var(
	G_config *Config
)

func InitConfig(filename string)(err error){

	var(
		content []byte
		Conf Config
	)

	//1. 把配置文件读进来
	if content, err = ioutil.ReadFile(filename);err!=nil{
		return
	}

	//2. 反序列化
	if err = json.Unmarshal(content, &Conf);err!=nil{
		return
	}

	//3. 赋值单例
	G_config = &Conf

	return

}
