package updater

import (
	"os"
	"testing"
)

func Test_API(_t *testing.T) {
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

	bucket, err := FindBucket("updater")
	//查找不存在的bucket应该报错
	if nil == err {
		_t.Error(err)
	}
	//查找不存在的bucket返回空值
	if nil != bucket {
		_t.Error("FAIL FindBucket")
	}

	bucket, err = NewBucket("updater")
	//新建不存在的bucket不应该报错
	if nil != err {
		_t.Error(err)
	}

	_, err = NewBucket("updater")
	//新建存在的bucket应该报错
	if nil == err {
		_t.Error("FAIL NewBucket")
	}

	_, err = FindBucket("updater")
	//查找存在的bucket不应该报错
	if nil != err {
		_t.Error("FAIL FindBucket")
	}

	err = bucket.NewChannel("channel-01")
	//新建不存在的channel不应该报错
	if nil != err {
		_t.Error(err)
	}

	//新建存在的channel应该报错
	err = bucket.NewChannel("channel-01")
	if nil == err {
		_t.Error("FAIL NewChannel")
	}

	res1, err := bucket.Push("1/2/", "res.txt", []byte("0123456789"))
	//正确存放res不应该报错
	if nil != err {
		_t.Error(err)
	}

	res2, err := bucket.Push("1/", "res.txt", []byte("abcdefg"))
	//正确存放res不应该报错
	if nil != err {
		_t.Error(err)
	}

	//存放成功的res的uuid不应为空
	if res1 == "" {
		_t.Error("uuid is empty")
	}

	bytes, err := bucket.Pull(res1)
	//正确读取res不应该报错
	if nil != err {
		_t.Error(err)
	}

	//读取成功的res的内容应该匹配
	if string(bytes) != "0123456789" {
		_t.Error("res != 0123456789")
	}

	_, err = bucket.Find(res1)
	//查找存在的资源不应该报错
	if nil != err {
		_t.Error(err)
	}

	res, err := bucket.Find("0000000")
	//查找不存在的资源应该报错
	if nil != err {
		_t.Error(err)
	}
	//查找不存在的资源应该为空值
	if nil != res {
		_t.Error("FAIL Find")
	}

	err = bucket.Attach(res2, "channel-01")
	//附加存在的channel不应该报错
	if nil != err {
		_t.Error(err)
	}

	err = bucket.Attach(res2, "channel-03")
	//附加不存在的channel应该报错
	if nil == err {
		_t.Error("FAIL Attach")
	}

	c0, err := MakeJSON("updater", "")
	if nil != err {
		_t.Error(err)
	}
	_t.Log(string(c0))

	c1, err := MakeJSON("updater", "channel-01")
	if nil != err {
		_t.Error(err)
	}
	_t.Log(string(c1))

	err = bucket.Delete(res1)
	//删除存在的资源不应该报错
	if nil != err {
		_t.Error(err)
	}

	err = bucket.Delete("123456")
	//删除不存在的资源应该报错
	if nil == err {
		_t.Error("FAIL Delete")
	}

	err = bucket.Delete("123456")
	//删除不存在的资源应该报错
	if nil == err {
		_t.Error("FAIL Delete")
	}

	err = bucket.DeleteChannel("channel-01")
	//删除存在的channel不应该报错
	if nil != err {
		_t.Error(err)
	}

	err = bucket.DeleteChannel("channel-01")
	//删除不存在的channel应该报错
	if nil == err {
		_t.Error("FAIL DeleteChannel")
	}

	err = DeleteBucket("updater")
	//删除存在的bucket不应该报错
	if nil != err {
		_t.Error(err)
	}
	err = DeleteBucket("updater")
	//删除不存在的bucket应该报错
	if nil == err {
		_t.Error(err)
	}
}
