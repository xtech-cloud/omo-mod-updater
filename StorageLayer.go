package updater

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type IStorage interface {
	Setup(_config Config) error

	SaveBucket(_bucket *Bucket) error
	ReadBucket(_bucket *Bucket) (*Bucket, error)
	DeleteBucket(_bucket *Bucket) error

	WriteRes(_bucket *Bucket, _res *Res, _data []byte) error
	ReadRes(_bucket *Bucket, _res *Res) (*Res, error)
	ListRes(_bucket *Bucket) ([]*Res, error)
	DeleteRes(_bucket *Bucket, _res *Res) error
	ReadRaw(_bucket *Bucket, _res *Res) ([]byte, error)
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
	_bucket.UUID = uuid

	return err
}

func (_self *FileLayer) ReadBucket(_bucket *Bucket) (*Bucket, error) {
	uuid := _self.takeBucketUUID(_bucket)
	file := fmt.Sprintf("%s%s.bkt", _self.Conf.File.RootPath, uuid)
	data, err := ioutil.ReadFile(file)
	if nil != err {
		return nil, err
	}

	var bucket Bucket
	err = json.Unmarshal(data, &bucket)
	return &bucket, err
}

func (_self *FileLayer) DeleteBucket(_bucket *Bucket) error {
	uuid := _self.takeBucketUUID(_bucket)
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
	bucket := _self.takeBucketUUID(_bucket)
	//补齐字段
	_res.UUID = _self.makeUUID(_res)
	_res.MD5 = _self.makeMD5(_data)
	_res.Size = len(_data)

	//生成meta
	meta, err := json.Marshal(_res)
	if nil != err {
		return err
	}

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

func (_self *FileLayer) ReadRaw(_bucket *Bucket, _res *Res) ([]byte, error) {
	bucketID := _self.takeBucketUUID(_bucket)
	resID := _self.takeResUUID(_res)
	metafile := fmt.Sprintf("%s%s/%s.meta", _self.Conf.File.RootPath, bucketID, resID)
	meta, err := ioutil.ReadFile(metafile)
	if nil != err {
		return make([]byte, 0), err
	}

	err = json.Unmarshal(meta, _res)
	if nil != err {
		return make([]byte, 0), err
	}

	binfile := fmt.Sprintf("%s%s%s%s", _self.Conf.File.DataPath, bucketID, _res.Path, _res.File)
	return ioutil.ReadFile(binfile)
}

func (_self *FileLayer) ListRes(_bucket *Bucket) ([]*Res, error) {
	bucket := _self.takeBucketUUID(_bucket)
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

func (_self *FileLayer) ReadRes(_bucket *Bucket, _res *Res) (*Res, error) {
	bucketID := _self.takeBucketUUID(_bucket)
	resID := _self.takeResUUID(_res)

	metadir := fmt.Sprintf("%s%s", _self.Conf.File.RootPath, bucketID)
	fis, err := ioutil.ReadDir(metadir)
	if nil != err {
		return nil, err
	}

	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}

		if fi.Name() != resID+".meta" {
			continue
		}

		metafile := fmt.Sprintf("%s%s/%s", _self.Conf.File.RootPath, bucketID, fi.Name())
		meta, err := ioutil.ReadFile(metafile)
		if nil != err {
			return nil, err
		}

		var res Res
		err = json.Unmarshal(meta, &res)
		if nil != err {
			return nil, err
		}
		return &res, nil
	}
	return nil, nil
}

func (_self *FileLayer) DeleteRes(_bucket *Bucket, _res *Res) error {
	bucketID := _self.takeBucketUUID(_bucket)

	res, err := _self.ReadRes(_bucket, _res)
	if nil != err {
		return err
	}

	if nil == res {
		return errors.New("not found")
	}

	binfile := fmt.Sprintf("%s%s%s%s", _self.Conf.File.DataPath, bucketID, res.Path, res.File)
	err = os.Remove(binfile)
	if nil != err {
		return err
	}
	metafile := fmt.Sprintf("%s%s/%s.meta", _self.Conf.File.RootPath, bucketID, res.UUID)
	err = os.Remove(metafile)
	if nil != err {
		return err
	}
	return nil
}

/// \brief filelayer的bucket的uuid是用name的MD5生成的
func (_self *FileLayer) takeBucketUUID(_bucket *Bucket) string {
	uuid := _bucket.UUID
	if uuid == "" {
		uuid = _self.makeMD5([]byte(_bucket.Name))
	}
	return uuid
}

func (_self *FileLayer) takeResUUID(_res *Res) string {
	uuid := _res.UUID
	if uuid == "" {
		uuid = _self.makeUUID(_res)
	}
	return uuid
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
