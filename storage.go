package storage

import (
	"fmt"
	"io/ioutil"
	"os"
)

const (
	// FS local file system
	FS = iota
	// S3 remote storage is AWS S3 (FIXME: not implemented)
	S3 = iota
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

func FSCreate(fname string, src []byte) error {
	fp, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(src)
	if err != nil {
		return fmt.Errorf("%s, %s", fname, err)
	}
	return nil
}

func FSRead(fname string) ([]byte, error) {
	return ioutil.ReadFile(fname)
}

func FSUpdate(fname string, src []byte) error {
	fp, err := os.OpenFile(fname, os.O_RDWR|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fp.Write(src)
	if err != nil {
		return fmt.Errorf("%s, %s", fname, err)
	}
	return nil
}

func FSDelete(fname string) error {
	return os.Remove(fname)
}

func Init(storageType int) (*Site, error) {
	switch storageType {
	case FS:
		return &Site{
			Config: map[string]interface{}{},
			Create: FSCreate,
			Read:   FSRead,
			Update: FSUpdate,
			Delete: FSDelete,
			Close:  func() error { return nil },
		}, nil
	case S3:
		return nil, fmt.Errorf("S3 storageType not implemented")
	default:
		return nil, fmt.Errorf("storageType not supported")
	}
}
