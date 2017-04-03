package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	// 3rd Party Packages
	/*
		"github.com/aws/aws-sdk-go/aws"
		"github.com/aws/aws-sdk-go/aws/session"
		"github.com/aws/aws-sdk-go/service/s3"
	*/)

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

// FSCreate creates a new file on the file system with a given name from the byte array.
func FSCreate(fname string, src []byte) error {
	// FIXME: FSCreate should create the path elements if necessary
	dname := path.Dir(fname)
	os.MkdirAll(dname, 0775)
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

// FSRead reads the file from the file system and returns a byte array and error.
func FSRead(fname string) ([]byte, error) {
	return ioutil.ReadFile(fname)
}

// FSUpdate replaces a file on the file system with the contents fo byte array returning error.
// It will truncate the file if necessary.
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

// FSDelete removes a file from the file system returning an error if needed.
func FSDelete(fname string) error {
	return os.Remove(fname)
}

// S3Init is a function that initialize an AWS/S3 session
func S3Init(options map[string]interface{}) (*Site, error) {
	return nil, fmt.Errorf("S3Init(%+v) not implemented", options)
}

// S3Create takes a relative path and a byte array of content and writes it to the bucket
// associated with the Site initialized.
func S3Create(fname string, src []byte) error {
	return fmt.Errorf("S3Create() not implemented")
}

// S3Read takes a relative path and returns a byte array and error from the bucket read
func S3Read(fname string) ([]byte, error) {
	return nil, fmt.Errorf("S3Read() not implemented")
}

// S3Update takes a relative path and a byte array of content and writes it to the bucket
// associated with the Site initialized.
func S3Update(fname string, src []byte) error {
	return fmt.Errorf("S3Update() not implemented")
}

// S3Delete takes a relative path and returns an error if delete not successful
func S3Delete(fname string) error {
	return fmt.Errorf("S3Delete() not implemented")
}

// S3Close closes a AWS S3 session
func S3Close() error {
	return fmt.Errorf("S3Close() not implemented")
}

// Init returns a *Site structure that points to configuration info (e.g. S3 credentials)
// and basic CRUD functions associated with the Site's storage type.
func Init(storageType int, options map[string]interface{}) (*Site, error) {
	switch storageType {
	case FS:
		return &Site{
			Config: options,
			Create: FSCreate,
			Read:   FSRead,
			Update: FSUpdate,
			Delete: FSDelete,
			Close:  func() error { return nil },
		}, nil
	case S3:
		return S3Init(options)
	default:
		return nil, fmt.Errorf("storageType not supported")
	}
}
