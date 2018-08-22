package updater

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type ReqFetch struct {
	Bucket  string `json:"bucket"`
	Channel string `json:"channel"`
}

func Serve(_addr string, _dataPath string, _bucket string) {
	bucket, _ := FindBucket(_bucket)
	if nil == bucket {
		panic("bucket not found")
	}
	upgradeHandler := http.StripPrefix("/upgrade/", http.FileServer(http.Dir(_dataPath+bucket.UUID)))

	http.HandleFunc("/upgrade/", func(_w http.ResponseWriter, _r *http.Request) {
		upgradeHandler.ServeHTTP(_w, _r)
	})

	http.HandleFunc("/fetch", func(_w http.ResponseWriter, _r *http.Request) {
		if _r.Method != "POST" {
			http.Error(_w, "only support POST", http.StatusNotFound)
			return
		}

		body, err := ioutil.ReadAll(_r.Body)
		if nil != err {
			http.Error(_w, err.Error(), http.StatusInternalServerError)
			return
		}
		var req ReqFetch
		err = json.Unmarshal(body, &req)
		if nil != err {
			http.Error(_w, err.Error(), http.StatusInternalServerError)
			return
		}
		manifest, err := MakeJSON(req.Bucket, req.Channel)
		if nil != err {
			http.Error(_w, err.Error(), http.StatusInternalServerError)
			return
		}
		_w.Write(manifest)
	})

	err := http.ListenAndServe(_addr, nil)
	if nil != err {
		panic(err)
	}
}
