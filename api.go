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
	UUID string `json:"uuid"`
	Name string `json:"name"`
}

var storage IStorage

/// \brief 配置
func Setup(_config Config) error {
	if _config.Layer == "file" {
		fileLayer := &FileLayer{}
		storage = fileLayer
		return fileLayer.Setup(_config)
	}
	return errors.New("layer only support file and mongo")
}

/// \brief 生成JSON格式的清单
/// \note _channel为空时，生成所有的资源的清单
func MakeJSON(_bucket string, _channel string) ([]byte, error) {
	bucket, err := FindBucket(_bucket)
	if nil != err {
		return make([]byte, 0), err
	}

	resAry, err := bucket.List(_channel)
	if nil != err {
		return make([]byte, 0), err
	}

	return json.Marshal(resAry)
}

/// \brief 新建bucket
func NewBucket(_name string) (*Bucket, error) {
	b := &Bucket{
		Name: _name,
	}

	bucket, _ := storage.ReadBucket(b)
	if nil != bucket {
		return nil, errors.New("bucket exists")
	}

	err := storage.WriteBucket(b)
	return b, err
}

/// \brief 删除bucket
func DeleteBucket(_name string) error {
	b := &Bucket{
		Name: _name,
	}

	bucket, _ := storage.ReadBucket(b)
	if nil == bucket {
		return errors.New("bucket not exists")
	}
	return storage.DeleteBucket(bucket)
}

/// \brief 查找bucket
func FindBucket(_name string) (*Bucket, error) {
	bucket := &Bucket{
		Name: _name,
	}
	return storage.ReadBucket(bucket)
}

/// \brief 新建channel
func (_self *Bucket) NewChannel(_name string) error {
	c := &Channel{
		Name: _name,
	}
	channel, _ := storage.ReadChannel(_self, c)
	if nil != channel {
		return errors.New("channel exists")
	}
	return storage.WriteChannel(_self, c)
}

/// \brief 删除channel
func (_self *Bucket) DeleteChannel(_name string) error {
	channel := &Channel{
		Name: _name,
	}
	return storage.DeleteChannel(_self, channel)
}

/// \brief
func (_self *Bucket) List(_channel string) ([]*Res, error) {
	if _channel == "" {
		return storage.ListRes(_self)
	}
	channel := &Channel{
		Name: _channel,
	}
	return storage.Filter(_self, channel)
}

/// \brief 存res
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

/// \brief 取res
/// \return
/// (path, file, data, error)
func (_self *Bucket) Pull(_uuid string) ([]byte, error) {
	res := &Res{
		UUID: _uuid,
	}
	return storage.ReadRaw(_self, res)
}

/// \brief 查找res
func (_self *Bucket) Find(_uuid string) (*Res, error) {
	res := &Res{
		UUID: _uuid,
	}
	return storage.ReadRes(_self, res)
}

/// \brief 删除res
func (_self *Bucket) Delete(_uuid string) error {
	res := &Res{
		UUID: _uuid,
	}
	return storage.DeleteRes(_self, res)
}

/// \brief 将res附加到channel中
func (_self *Bucket) Attach(_uuid string, _channel string) error {
	res := &Res{
		UUID: _uuid,
	}
	channel := &Channel{
		Name: _channel,
	}
	return storage.Attach(_self, res, channel)
}

/// \brief 将res从channel中分离
func (_self *Bucket) Detach(_uuid string, _channel string) error {
	res := &Res{
		UUID: _uuid,
	}
	channel := &Channel{
		Name: _channel,
	}
	return storage.Detach(_self, res, channel)
}
