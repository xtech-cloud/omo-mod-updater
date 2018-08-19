package updater

import (
	"encoding/json"
	"errors"
	"strings"
)

type FileConfig struct {
	RootPath string /// 根路径
	DataPath string /// 数据存放路径
}

type MongoConfig struct {
}

type Config struct {
	Layer string /// file or mongo
	File  FileConfig
	Mongo MongoConfig
}

type Res struct {
	UUID string `json:"uuid"`
	File string `json:"file"`
	Path string `json:"path"`
	MD5  string `json:"md5"`
	Size int    `json:"size"`
}

type Channel struct {
	Name string `json:"name"`
}

type Bucket struct {
	Name     string     `json:"name"`
	Channels []*Channel `json:"channels"`
}

var storage IStorage

func Setup(_config Config) error {
	if _config.Layer == "file" {
		fileLayer := &FileLayer{}
		storage = fileLayer
		return fileLayer.Setup(_config)
	}
	return errors.New("layer only support file and mongo")
}

func MakeJSON(_bucket string, _channel string) ([]byte, error) {
	bucket, err := FindBucket(_bucket)
	if nil != err {
		return make([]byte, 0), err
	}

	resAry, err := bucket.List()
	if nil != err {
		return make([]byte, 0), err
	}

	return json.Marshal(resAry)
}

func NewBucket(_name string) (*Bucket, error) {
	bucket := &Bucket{
		Name:     _name,
		Channels: make([]*Channel, 0),
	}

	exists := storage.HasBucket(bucket)
	if exists {
		return nil, errors.New("bucket exists")
	}

	err := storage.SaveBucket(bucket)
	return bucket, err
}

func DeleteBucket(_name string) error {
	bucket := &Bucket{
		Name: _name,
	}
	return storage.DeleteBucket(bucket)
}

func FindBucket(_name string) (*Bucket, error) {
	bucket := &Bucket{
		Name: _name,
	}
	err := storage.ReadBucket(bucket)
	return bucket, err
}

func ListBucket() ([]*Bucket, error) {
	return nil, nil
}

func (_self *Bucket) NewChannel(_name string) error {
	channel := &Channel{
		Name: _name,
	}
	for _, channel := range _self.Channels {
		if channel.Name == _name {
			return errors.New("channel exists")
		}
	}
	_self.Channels = append(_self.Channels, channel)
	err := storage.SaveBucket(_self)
	return err
}

func (_self *Bucket) List() ([]*Res, error) {
	return storage.ListRes(_self)
}

/// \return
/// (uuid, error)
func (_self *Bucket) Push(_path string, _file string, _data []byte) (string, error) {
	res := &Res{
		Path: _path,
		File: _file,
	}
	if !strings.HasPrefix(res.Path, "/") {
		res.Path = "/" + res.Path
	}
	if !strings.HasSuffix(res.Path, "/") {
		res.Path = res.Path + "/"
	}
	err := storage.WriteRes(_self, res, _data)
	return res.UUID, err
}

/// \return
/// (path, file, data, error)
func (_self *Bucket) Pull(_uuid string) ([]byte, error) {
	res := &Res{
		UUID: _uuid,
	}
	return storage.ReadRes(_self, res)
}
