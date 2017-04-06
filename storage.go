package storage

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	// 3rd Party Packages
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
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
	Create func(string, io.Reader) error
	Read   func(string) ([]byte, error)
	Update func(string, io.Reader) error
	Delete func(string) error
	Close  func() error
}

// FSInit initialize a file system type site
func FSInit(options map[string]interface{}) (*Site, error) {
	s := new(Site)
	s.Config = options
	s.Create = func(fname string, rd io.Reader) error {
		return FSCreate(s, fname, rd)
	}
	s.Read = func(fname string) ([]byte, error) {
		return FSRead(s, fname)
	}
	s.Update = func(fname string, rd io.Reader) error {
		return FSUpdate(s, fname, rd)
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
func FSCreate(s *Site, fname string, rd io.Reader) error {
	// FIXME: FSCreate should create the path elements only if necessary
	dname := path.Dir(fname)
	os.MkdirAll(dname, 0775)
	wr, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer wr.Close()
	_, err = io.Copy(wr, rd)
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
func FSUpdate(s *Site, fname string, rd io.Reader) error {
	wr, err := os.OpenFile(fname, os.O_RDWR|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer wr.Close()
	_, err = io.Copy(wr, rd)
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
	var (
		awsSDKLoadConfig       bool
		awsSharedConfigEnabled bool
		awsProfile             string

		sess *session.Session
		err  error
	)

	site := new(Site)
	site.Config = map[string]interface{}{}
	// Copy the values into site.Config so they can travel with the struct
	for key, val := range options {
		site.Config[key] = val
	}

	if val, ok := options["AwsSDKLoadConfig"]; ok == true {
		awsSDKLoadConfig = val.(bool)
	}
	if val, ok := options["AwsSharedConfigEnabled"]; ok == true {
		awsSharedConfigEnabled = val.(bool)
	}

	if awsSDKLoadConfig == true {
		var opts session.Options
		if awsProfile != "" {
			opts.Profile = awsProfile
		} else {
			opts.Profile = "default"
		}
		if awsSharedConfigEnabled == true {
			opts.SharedConfigState = session.SharedConfigEnable
		} else {
			opts.SharedConfigState = session.SharedConfigDisable
		}
		if val, ok := options["AwsRegion"]; ok == true {
			opts.Config = aws.Config{Region: aws.String(val.(string))}
		}

		sess, err = session.NewSessionWithOptions(opts)
		if err != nil {
			return nil, err
		}
	} else {
		sess, err = session.NewSession()
		if err != nil {
			return nil, err
		}
	}
	site.Config["session"] = sess

	s3Svc := s3.New(sess)
	site.Config["s3Service"] = s3Svc

	site.Create = func(fname string, rd io.Reader) error {
		return S3Create(site, fname, rd)
	}
	site.Read = func(fname string) ([]byte, error) {
		return S3Read(site, fname)
	}
	site.Update = func(fname string, rd io.Reader) error {
		return S3Update(site, fname, rd)
	}
	site.Delete = func(fname string) error {
		return S3Delete(site, fname)
	}
	site.Close = func() error {
		return S3Close(site)
	}
	return site, nil
}

// Create takes a relative path and a byte array of content and writes it to the bucket
// associated with the Site initialized.
func S3Create(s *Site, fname string, rd io.Reader) error {
	if val, ok := s.Config["s3Service"]; ok == true {
		s3Svc := val.(s3iface.S3API)
		if _, ok := s.Config["AwsBucket"]; ok == false {
			return fmt.Errorf("Bucket not defined for %s", fname)
		}
		bucketName := s.Config["AwsBucket"].(string)
		upParams := &s3manager.UploadInput{
			Bucket: &bucketName,
			Key:    &fname,
			Body:   rd,
		}
		uploader := s3manager.NewUploaderWithClient(s3Svc)
		_, err := uploader.Upload(upParams)
		if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("s3Service object not available")
}

// S3Read takes a relative path and returns a byte array and error from the bucket read
func S3Read(s *Site, fname string) ([]byte, error) {
	if val, ok := s.Config["s3Service"]; ok == true {
		s3Svc := val.(s3iface.S3API)
		if _, ok := s.Config["AwsBucket"]; ok == false {
			return nil, fmt.Errorf("Bucket not defined for %s", fname)
		}
		bucketName := s.Config["AwsBucket"].(string)
		downloadParams := &s3.GetObjectInput{
			Bucket: &bucketName,
			Key:    &fname,
		}
		buf := &aws.WriteAtBuffer{}
		downloader := s3manager.NewDownloaderWithClient(s3Svc)
		_, err := downloader.Download(buf, downloadParams)
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return nil, fmt.Errorf("s3Service object not available")
}

// S3Update takes a relative path and a byte array of content and writes it to the bucket
// associated with the Site initialized.
func S3Update(s *Site, fname string, rd io.Reader) error {
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
		return nil, fmt.Errorf("storageType not supported")
	}
}
