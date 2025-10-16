package model

type FileStatus int

const (
	StatusNew FileStatus = iota
	StatusSynced
	StatusDeletedInSource
	StatusTempDeleted
	StatusError
)
