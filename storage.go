package storage

import (
	"fmt"
)

const (
	// S3 indicates the remote storage is AWS S3
	S3 = iota
	// IPFS Indicates the remote storage is IPFS based
	IPFS = iota
	// SFTP
	SFTP = iota
	// SSH
	SSH = iota
)

// Site wrapps the given system interface normalizing to simple Create, Read, Update, Delete operations
type Site struct {
	// Attributes holds any data needed for managing the remote session for desired operations
	Config map[string]interface{}
	Create func(string, []byte) error
	Read   func(string) ([]byte, error)
	Update func(string, []byte) error
	Delete func(string) error
	Close  func() error
}

func Init(storageType int) (*Site, error) {
	return nil, fmt.Errorf("Init(%d) not implemented", storageType)
}
