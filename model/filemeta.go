package model

type FileMeta struct {
	Size    int64      `json:"size"`
	ModTime int64      `json:"mtime"`
	Hash    string     `json:"hash"`
	Status  FileStatus `json:"status"`
}

type RemoteFile struct {
	Key     string
	Hash    string
	Size    int64
	ModTime int64
}
