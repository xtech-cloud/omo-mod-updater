package updater

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type IStorage interface {
	Setup(_config Config) error

	HasBucket(_bucket *Bucket) bool
	SaveBucket(_bucket *Bucket) error
	ReadBucket(_bucket *Bucket) error
	DeleteBucket(_bucket *Bucket) error

	WriteRes(_bucket *Bucket, _res *Res, _data []byte) error
	ReadRes(_bucket *Bucket, _res *Res) ([]byte, error)
	ListRes(_bucket *Bucket) ([]*Res, error)
}

type IOLayer struct {
	Conf Config
}

type FileLayer struct {
	IOLayer
}

func (_self *FileLayer) Setup(_config Config) error {
	_self.Conf = _config

	// create rootpath
	if !strings.HasSuffix(_self.Conf.File.RootPath, "/") {
		_self.Conf.File.RootPath = _self.Conf.File.RootPath + "/"
	}
	err := os.MkdirAll(_self.Conf.File.RootPath, 0666)
	if nil != err {
		return err
	}

	// create datapath
	if !strings.HasSuffix(_self.Conf.File.DataPath, "/") {
		_self.Conf.File.DataPath = _self.Conf.File.DataPath + "/"
	}
	err = os.MkdirAll(_self.Conf.File.DataPath, 0666)
	if nil != err {
		return err
	}

	return nil
}

func (_self *FileLayer) SaveBucket(_bucket *Bucket) error {
	bytes, err := json.Marshal(_bucket)
	if nil != err {
		return err
	}

	uuid := _self.makeMD5([]byte(_bucket.Name))
	os.MkdirAll(_self.Conf.File.DataPath+uuid, 0666)
	os.MkdirAll(_self.Conf.File.RootPath+uuid, 0666)
	file := fmt.Sprintf("%s%s.bkt", _self.Conf.File.RootPath, uuid)
	err = ioutil.WriteFile(file, bytes, 0644)
	if nil != err {
		return err
	}

	return err
}

func (_self *FileLayer) HasBucket(_bucket *Bucket) bool {
	uuid := _self.makeMD5([]byte(_bucket.Name))

	file := fmt.Sprintf("%s%s.bkt", _self.Conf.File.RootPath, uuid)
	_, err := os.Stat(file)
	if nil != err && os.IsNotExist(err) {
		return false
	}
	return true
}

func (_self *FileLayer) ReadBucket(_bucket *Bucket) error {
	uuid := _self.makeMD5([]byte(_bucket.Name))
	file := fmt.Sprintf("%s%s.bkt", _self.Conf.File.RootPath, uuid)
	data, err := ioutil.ReadFile(file)
	if nil != err {
		return err
	}

	return json.Unmarshal(data, _bucket)
}

func (_self *FileLayer) DeleteBucket(_bucket *Bucket) error {
	uuid := _self.makeMD5([]byte(_bucket.Name))
	file := fmt.Sprintf("%s%s.bkt", _self.Conf.File.RootPath, uuid)
	err := os.RemoveAll(_self.Conf.File.DataPath + uuid + "/")
	if nil != err {
		return err
	}
	err = os.RemoveAll(_self.Conf.File.RootPath + uuid + "/")
	if nil != err {
		return err
	}
	return os.Remove(file)
}

func (_self *FileLayer) WriteRes(_bucket *Bucket, _res *Res, _data []byte) error {
	//补齐字段
	_res.UUID = _self.makeUUID(_res)
	_res.MD5 = _self.makeMD5(_data)
	_res.Size = len(_data)

	//生成meta
	meta, err := json.Marshal(_res)
	if nil != err {
		return err
	}

	bucket := _self.makeMD5([]byte(_bucket.Name))

	//save file
	binfile := fmt.Sprintf("%s%s%s%s", _self.Conf.File.DataPath, bucket, _res.Path, _res.File)
	dir := filepath.Dir(binfile)
	os.MkdirAll(dir, 0666)
	err = ioutil.WriteFile(binfile, _data, 0644)
	if nil != err {
		return err
	}

	//save meta
	metafile := fmt.Sprintf("%s%s/%s.meta", _self.Conf.File.RootPath, bucket, _res.UUID)
	err = ioutil.WriteFile(metafile, meta, 0644)
	if nil != err {
		return err
	}

	return nil
}

func (_self *FileLayer) ReadRes(_bucket *Bucket, _res *Res) ([]byte, error) {
	bucket := _self.makeMD5([]byte(_bucket.Name))
	metafile := fmt.Sprintf("%s%s/%s.meta", _self.Conf.File.RootPath, bucket, _res.UUID)
	meta, err := ioutil.ReadFile(metafile)
	if nil != err {
		return make([]byte, 0), err
	}

	err = json.Unmarshal(meta, _res)
	if nil != err {
		return make([]byte, 0), err
	}

	binfile := fmt.Sprintf("%s%s%s%s", _self.Conf.File.DataPath, bucket, _res.Path, _res.File)
	return ioutil.ReadFile(binfile)
}

func (_self *FileLayer) ListRes(_bucket *Bucket) ([]*Res, error) {
	bucket := _self.makeMD5([]byte(_bucket.Name))
	metadir := fmt.Sprintf("%s%s", _self.Conf.File.RootPath, bucket)
	fis, err := ioutil.ReadDir(metadir)
	if nil != err {
		return make([]*Res, 0), err
	}

	resAry := make([]*Res, 0)
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}

		if !strings.HasSuffix(fi.Name(), ".meta") {
			continue
		}

		metafile := fmt.Sprintf("%s%s/%s", _self.Conf.File.RootPath, bucket, fi.Name())
		meta, err := ioutil.ReadFile(metafile)
		if nil != err {
			return make([]*Res, 0), err
		}

		var res Res
		err = json.Unmarshal(meta, &res)
		if nil != err {
			return make([]*Res, 0), err
		}

		resAry = append(resAry, &res)

	}
	return resAry, nil
}

func (_self *FileLayer) makeUUID(_res *Res) string {
	h := md5.New()
	h.Write([]byte(_res.Path + _res.File))
	return hex.EncodeToString(h.Sum(nil))
}

func (_self *FileLayer) makeMD5(_data []byte) string {
	h := md5.New()
	h.Write(_data)
	return hex.EncodeToString(h.Sum(nil))
}
