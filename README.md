# API

## 配置

```go
config := &updater.Config {
    Layer: "file",
    FileConfig: {
        RootPath: "/tmp/updater/root/",
        DataPath: "/tmp/updater/data/",
    }
}

updater.Setup(config)
```

## 存入资源

```go
bucket, _ := updater.NewBucket("bucket")
uuid, err := bucket.Push("/res/", "1.txt", []byte("0123456789"))
```

## 放入通道
```go
bucket, _ := updater.FindBucket("bucket")
resID := "38b8c2c1093dd0fec383a9d9ac940515"
channelName := "channel"
bucket.Attach(resID, channelName")

```

## 获取清单

```go
manifest, err := updater.MakeJSON("bucket", "channel")
```

## 取出资源

```go
bucket, _ := updater.FindBucket("bucket")
bytes, err := updater.Pull(uuid)

```

# 说明

## bucket 

bucket是物理隔离的空间，用于存放res文件

## channel

channel是bucket中的逻辑隔离的空间，用于将一个bucket中的文件分隔成不同的通道，一个res文件可以存在于多个channel中。

## res

res是一个实体文件

