package updater

import (
	"os"
	"testing"
)

func Test_Setup(_t *testing.T) {
	os.RemoveAll("/tmp/updater")

	config := Config{
		Layer: "file",
		File: FileConfig{
			RootPath: "/tmp/updater/root/",
			DataPath: "/tmp/updater/data/",
		},
	}
	err := Setup(config)
	if nil != err {
		_t.Error(err)
	}
}

func Test_Bucket(_t *testing.T) {
	bucket, err := NewBucket("updater")
	if nil != err {
		_t.Error(err)
	}

	//新建存在的bucket应该报错
	_, err = NewBucket("updater")
	if nil == err {
		_t.Error("err == nil")
	}

	err = bucket.NewChannel("channel-01")
	if nil != err {
		_t.Error(err)
	}

	err = bucket.NewChannel("channel-02")
	if nil != err {
		_t.Error(err)
	}

	//新建存在的channel应该报错
	err = bucket.NewChannel("channel-02")
	if nil == err {
		_t.Error("err == nil")
	}

	bucket, err = FindBucket("updater")
	if nil != err {
		_t.Error(err)
	}

	if len(bucket.Channels) == 0 {
		_t.Error("len(channels) is 0")
	}

	_, err = FindBucket("updater2")
	if nil == err {
		_t.Error("err == nil")
	}
}

func Test_Res(_t *testing.T) {
	bucket, err := FindBucket("updater")
	if nil != err {
		_t.Error(err)
	}

	uuid, err := bucket.Push("1/2/", "res.txt", []byte("0123456789"))
	if nil != err {
		_t.Error(err)
	}

	if uuid == "" {
		_t.Error("uuid is empty")
	}

	bytes, err := bucket.Pull(uuid)
	if nil != err {
		_t.Error(err)
	}
	if string(bytes) != "0123456789" {
		_t.Error("res != 0123456789")
	}

	_, err = bucket.Find(uuid)
	if nil != err {
		_t.Error(err)
	}

	_, err = bucket.Find("0000000")
	if nil == err {
		_t.Error("err == nil")
	}
}

func Test_Manifest(_t *testing.T) {
	_, err := MakeJSON("updater", "")
	if nil != err {
		_t.Error(err)
	}
}

func Test_DeleteBucket(_t *testing.T) {
	err := DeleteBucket("updater")
	if nil != err {
		_t.Error(err)
	}
	//删除不存在的bucket应该报错
	err = DeleteBucket("updater")
	if nil == err {
		_t.Error(err)
	}
}
