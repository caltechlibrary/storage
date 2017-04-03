package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	// 3rd Party Packages
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	// FS local file system
	FS = iota
	// S3 remote storage via AWS S3
	S3 = iota
)

// Site wrapps the given system interface normalizing to simple Create, Read, Update, Delete operations
type Site struct {
	// Attributes holds any data needed for managing the remote session for desired operations
	Config map[string]interface{}
	// Operations
	Create func(string, []byte) error
	Read   func(string) ([]byte, error)
	Update func(string, []byte) error
	Delete func(string) error
	Close  func() error
}

// FSInit initialize a file system type site
func FSInit(s *Site, options map[string]interface{}) (*Site, error) {
	s.Config = options
	s.Create = func(fname string, data []byte) error {
		return FSCreate(s, fname, data)
	}
	s.Read = func(fname string) ([]byte, error) {
		return FSRead(s, fname)
	}
	s.Update = func(fname string, data []byte) error {
		return FSUpdate(s, fname, data)
	}
	s.Delete = func(fname string) error {
		return FSDelete(s, fname)
	}
	s.Close = func() error {
		return nil
	}
	return s, nil
}

// FSCreate creates a new file on the file system with a given name from the byte array.
func FSCreate(s *Site, fname string, src []byte) error {
	// FIXME: FSCreate should create the path elements only if necessary
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
func FSRead(s *Site, fname string) ([]byte, error) {
	return ioutil.ReadFile(fname)
}

// FSUpdate replaces a file on the file system with the contents fo byte array returning error.
// It will truncate the file if necessary.
func FSUpdate(s *Site, fname string, src []byte) error {
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
func FSDelete(s *Site, fname string) error {
	return os.Remove(fname)
}

// Close closes a file system type site
func FSClose(s *Site) error {
	return nil
}

// S3Init is a function that initialize an AWS/S3 session
func S3Init(options map[string]interface{}) (*Site, error) {
	cfg := map[string]interface{}{}
	if val, ok := options["Bucket"]; ok == true {
		cfg["Bucket"] = val
	}

	//FIXME: Apply options to new session if values exist
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	s3Svc := s3.New(sess)

	cfg["session"] = sess
	cfg["s3Scv"] = s3Svc

	s := new(Site)
	s.Config = cfg
	s.Create = func(fname string, data []byte) error {
		return S3Create(s, fname, data)
	}
	s.Read = func(fname string) ([]byte, error) {
		return S3Read(s, fname)
	}
	s.Update = func(fname string, data []byte) error {
		return S3Update(s, fname, data)
	}
	s.Delete = func(fname string) error {
		return S3Delete(s, fname)
	}
	s.Close = func() error {
		return S3Close(s)
	}
	return s, nil
}

// Create takes a relative path and a byte array of content and writes it to the bucket
// associated with the Site initialized.
func S3Create(s *Site, fname string, src []byte) error {
	val, ok := s.Config["Bucket"]
	if ok == false {
		return fmt.Errorf("Bucket not defined for %s", fname)
	}
	bucketName := fmt.Sprintf("%s", val.(string))
	upParams := &s3manager.UploadInput{
		Bucket: &bucketName,
		Key:    &fname,
		Body:   src,
	}
	_, err := uploader.Upload(upParams)
	if err != nil {
		return err
	}
	return nil
}

// S3Read takes a relative path and returns a byte array and error from the bucket read
func S3Read(s *Site, fname string) ([]byte, error) {
	return nil, fmt.Errorf("S3Read() not implemented")
}

// S3Update takes a relative path and a byte array of content and writes it to the bucket
// associated with the Site initialized.
func S3Update(s *Site, fname string, src []byte) error {
	return fmt.Errorf("S3Update() not implemented")
}

// Delete takes a relative path and returns an error if delete not successful
func S3Delete(s *Site, fname string) error {
	return fmt.Errorf("S3Delete() not implemented")
}

// Close closes a AWS S3 session
func S3Close(s *Site) error {
	return fmt.Errorf("S3Close() not implemented")
}

// Init returns a *Site structure that points to configuration info (e.g. S3 credentials)
// and basic CRUD functions associated with the Site's storage type.
func Init(storageType int, options map[string]interface{}) (*Site, error) {
	switch storageType {
	case FS:
		return FSInit(options)
	case S3:
		return S3Init(options)
	default:
		return fmt.Errorf("storageType not supported")
	}
}
