package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

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
	// Other constants will be create as other storage systems are implemented
)

// Site wrapps the given system interface normalizing to simple Create, Read, Update, Delete operations
type Site struct {
	// Attributes holds any data needed for managing the remote session for desired operations
	Config map[string]interface{}

	// Basic CRUD Operations
	Create func(string, io.Reader) error
	Read   func(string) ([]byte, error)
	Update func(string, io.Reader) error
	Delete func(string) error

	// Additional operations for compatibility with os.* and ioutil.*
	Stat      func(string) (os.FileInfo, error)
	Mkdir     func(string, os.FileMode) error
	MkdirAll  func(string, os.FileMode) error
	Remove    func(string) error
	RemoveAll func(string) error
	ReadFile  func(string) ([]byte, error)
	WriteFile func(string, []byte, os.FileMode) error
}

// S3ObjectInfo is a map so we can create a os.FileInfo compatible struct from S3 objects
type S3ObjectInfo struct {
	Info map[string]interface{}
}

// String returns a string representation of the object reported by ListObjects
func (d *S3ObjectInfo) String() string {
	src, err := json.Marshal(d.Info)
	if err != nil {
		return fmt.Sprintf("%+v", d.Info)
	}
	return string(src)
}

// Create a S3ObjectInfo struct from response Contents return by ListObjects on S3
func S3ToObjectInfo(o *s3.Object) *S3ObjectInfo {
	doc := new(S3ObjectInfo)
	doc.Info = map[string]interface{}{}
	doc.Info["ETag"] = o.ETag
	doc.Info["Key"] = o.Key
	doc.Info["LastModified"] = o.LastModified
	doc.Info["Owner"] = o.Owner
	doc.Info["Size"] = o.Size
	doc.Info["StorageClass"] = o.StorageClass
	return doc
}

// Name returns the Key after evaluating with path.Base() so we match os.FileInfo.Name()
func (d *S3ObjectInfo) Name() string {
	if val, ok := d.Info["Key"]; ok == true {
		p := val.(*string)
		return path.Base(*p)
	}
	return ""
}

// Size returns the size of an object reported by listing the object
func (d *S3ObjectInfo) Size() int64 {
	if val, ok := d.Info["Size"]; ok == true {
		size := val.(*int64)
		return *size
	}
	return int64(0)
}

// ModTime returns the value of LastModied reported by listing the object
func (d *S3ObjectInfo) ModTime() time.Time {
	if val, ok := d.Info["LastModified"]; ok == true {
		t := val.(*time.Time)
		return *t
	}

	return time.Time{}
}

// Mode returns the file mode but this doens't map to S3 so we return zero always
func (d *S3ObjectInfo) Mode() os.FileMode {
	return os.FileMode(0)
}

// IsDir returns false, S3 doens't support the concept of directories only keys in buckets
func (d *S3ObjectInfo) IsDir() bool {
	return false
}

// Sys() returns an system dependant interface...
func (d *S3ObjectInfo) Sys() interface{} {
	return nil
}

// FSInit initialize a file system type site
func FSInit(options map[string]interface{}) (*Site, error) {
	site := new(Site)
	site.Config = options

	// Basic CRUD ops
	site.Create = func(fname string, rd io.Reader) error {
		return FSCreate(site, fname, rd)
	}
	site.Read = func(fname string) ([]byte, error) {
		return ioutil.ReadFile(fname)
	}
	site.Update = func(fname string, rd io.Reader) error {
		return FSUpdate(site, fname, rd)
	}
	site.Delete = func(fname string) error {
		return os.Remove(fname)
	}

	// Extra ops for compatibility with os.* and ioutil.*
	site.Stat = func(fname string) (os.FileInfo, error) {
		return os.Stat(fname)
	}
	site.Mkdir = func(name string, perm os.FileMode) error {
		return os.Mkdir(name, perm)
	}
	site.MkdirAll = func(path string, perm os.FileMode) error {
		return os.MkdirAll(path, perm)
	}
	site.Remove = func(name string) error {
		return os.Remove(name)
	}
	site.RemoveAll = func(path string) error {
		return os.RemoveAll(path)
	}
	site.ReadFile = func(fname string) ([]byte, error) {
		return ioutil.ReadFile(fname)
	}
	site.WriteFile = func(fname string, data []byte, perm os.FileMode) error {
		return ioutil.WriteFile(fname, data, perm)
	}
	return site, nil
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

	// Basic Ops
	site.Create = func(fname string, rd io.Reader) error {
		return S3Create(site, fname, rd)
	}
	site.Read = func(fname string) ([]byte, error) {
		return S3Read(site, fname)
	}
	site.Update = func(fname string, rd io.Reader) error {
		// NOTE: Create and Update and the same in S3, Update overwrites the existing object
		return S3Create(site, fname, rd)
	}
	site.Delete = func(fname string) error {
		return S3Remove(site, fname)
	}

	// Extra ops for compatibilty with os.* and ioutil.*
	site.Stat = func(fname string) (os.FileInfo, error) {
		return S3Stat(site, fname)
	}
	site.Mkdir = func(name string, perm os.FileMode) error {
		//NOTE: S3 lacks the concept of directories, the fill path is the "Key" value info the bucket
		return nil
	}
	site.MkdirAll = func(path string, perm os.FileMode) error {
		//NOTE: S3 lacks the concept of directories, the fill path is the "Key" value info the bucket
		return nil
	}
	site.Remove = func(fname string) error {
		return S3Remove(site, fname)
	}
	site.RemoveAll = func(fname string) error {
		return S3RemoveAll(site, fname)
	}
	site.ReadFile = func(fname string) ([]byte, error) {
		return S3Read(site, fname)
	}
	site.WriteFile = func(fname string, data []byte, perm os.FileMode) error {
		return S3Create(site, fname, bytes.NewBuffer(data))
	}
	return site, nil
}

// S3Stat takes a file name and returns a FileInfo and error value
func S3Stat(s *Site, fname string) (os.FileInfo, error) {
	if val, ok := s.Config["s3Service"]; ok == true {
		s3Svc := val.(s3iface.S3API)
		if _, ok := s.Config["AwsBucket"]; ok == false {
			return nil, fmt.Errorf("Bucket not defined for %s", fname)
		}
		bucketName := s.Config["AwsBucket"].(string)
		statParams := &s3.ListObjectsInput{
			Bucket:  &bucketName,
			Prefix:  &fname,
			MaxKeys: aws.Int64(1),
		}
		res, err := s3Svc.ListObjects(statParams)
		if err != nil {
			return nil, err
		}
		// NOTE: Only return the fname we're looking for not the other ones with matching prefix
		for _, obj := range res.Contents {
			oInfo := S3ToObjectInfo(obj)
			if strings.Compare(oInfo.Name(), path.Base(fname)) == 0 {
				return oInfo, nil
			}
		}
		return nil, fmt.Errorf("%s not found", fname)
	}
	return nil, fmt.Errorf("s3Service not configured")
}

// Create takes a full path and a byte array of content and writes it to the bucket
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
	return fmt.Errorf("s3Service not configured")
}

// S3Read takes a full path and returns a byte array and error from the bucket read
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
	return nil, fmt.Errorf("s3Service not configured")
}

// S3Remove takes a full path and returns an error if delete not successful
func S3Remove(s *Site, fname string) error {
	if val, ok := s.Config["s3Service"]; ok == true {
		s3Svc := val.(s3iface.S3API)
		if _, ok := s.Config["AwsBucket"]; ok == false {
			return fmt.Errorf("Bucket not defined for %s", fname)
		}
		bucketName := s.Config["AwsBucket"].(string)
		deleteParams := &s3.DeleteObjectInput{
			Bucket: &bucketName,
			Key:    &fname,
		}
		_, err := s3Svc.DeleteObject(deleteParams)
		return err
	}
	return fmt.Errorf("s3Service not configured")
}

// S3RemoveAll takes a path prefix and delete matching items (up to 1000) and returns an error if delete not successful
func S3RemoveAll(s *Site, prefixName string) error {
	if val, ok := s.Config["s3Service"]; ok == true {
		s3Svc := val.(s3iface.S3API)
		if _, ok := s.Config["AwsBucket"]; ok == false {
			return fmt.Errorf("Bucket not defined for %s", prefixName)
		}
		bucketName := s.Config["AwsBucket"].(string)
		// FIXME: Get a list of objects, then delate each one
		statParams := &s3.ListObjectsInput{
			Bucket: &bucketName,
			Prefix: &prefixName,
		}
		// S3 ListObjects returns an maximum of 1000 objects, I am using an outer loop to handle
		// the case of where the prefix matches more than 1000 objects.
		res, err := s3Svc.ListObjects(statParams)
		if err != nil {
			return err
		}
		cnt := len(res.Contents)
		for cnt > 0 {
			// NOTE: Only return the fname we're looking for not the other ones with matching prefix
			for _, obj := range res.Contents {
				deleteParams := &s3.DeleteObjectInput{
					Bucket: &bucketName,
					Key:    obj.Key,
				}
				_, err := s3Svc.DeleteObject(deleteParams)
				if err != nil {
					return err
				}
			}
			res, err := s3Svc.ListObjects(statParams)
			if err != nil {
				fmt.Printf("DEBUG this is the case where we're handling the deletion of more than 1000 objects for prefixName")
				return err
			}
			cnt = len(res.Contents)
		}
		return nil
	}
	return fmt.Errorf("s3Service not configured")
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
