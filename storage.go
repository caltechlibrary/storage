//
// storage package wraps both local disc and S3 storage with CRUD operations and common os.*, ioutil.* functions.
//
// @author R. S. Doiel, <rsdoiel@library.caltech.edu>
//
// Copyright (c) 2017, Caltech
// All rights not granted herein are expressly reserved by Caltech.
//
// Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
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
	// UNSUPPORTED is used if Init fails the and a non-nil Store struck gets returned.
	UNSUPPORTED = iota
	// FS local file system
	FS
	// S3 remote storage via AWS S3
	S3
	// Other constants will be create as other storage systems are implemented
)

// Store wrapps the given system interface normalizing to simple Create, Read, Update, Delete operations
type Store struct {
	// Attributes holds any data needed for managing the remote session for desired operations
	Config map[string]interface{}
	Type   int

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

// FSConfigure initialize a store to a local disc system type
func FSConfigure(store *Store) (*Store, error) {
	store.Type = FS

	// Basic CRUD ops
	store.Create = func(fname string, rd io.Reader) error {
		return FSCreate(store, fname, rd)
	}
	store.Read = func(fname string) ([]byte, error) {
		return ioutil.ReadFile(fname)
	}
	store.Update = func(fname string, rd io.Reader) error {
		return FSUpdate(store, fname, rd)
	}
	store.Delete = func(fname string) error {
		return os.Remove(fname)
	}

	// Extra ops for compatibility with os.* and ioutil.*
	store.Stat = func(fname string) (os.FileInfo, error) {
		return os.Stat(fname)
	}
	store.Mkdir = func(name string, perm os.FileMode) error {
		return os.Mkdir(name, perm)
	}
	store.MkdirAll = func(path string, perm os.FileMode) error {
		return os.MkdirAll(path, perm)
	}
	store.Remove = func(name string) error {
		return os.Remove(name)
	}
	store.RemoveAll = func(path string) error {
		return os.RemoveAll(path)
	}
	store.ReadFile = func(fname string) ([]byte, error) {
		return ioutil.ReadFile(fname)
	}
	store.WriteFile = func(fname string, data []byte, perm os.FileMode) error {
		return ioutil.WriteFile(fname, data, perm)
	}
	return store, nil
}

// FSCreate creates a new file on the file system with a given name from the byte array.
func FSCreate(s *Store, fname string, rd io.Reader) error {
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
func FSUpdate(s *Store, fname string, rd io.Reader) error {
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

// EnvToOptions given an environment map envvars to their option.
func EnvToOptions(env []string) map[string]interface{} {
	opts := map[string]interface{}{}
	for _, stmt := range env {
		if strings.HasPrefix(stmt, "AWS_") && strings.Contains(stmt, "=") {
			kv := strings.SplitN(stmt, "=", 2)
			switch kv[0] {
			case "AWS_SDK_LOAD_CONFIG":
				if kv[0] == "1" || strings.ToLower(kv[1]) == "true" {
					opts["AwsSDKLoadConfig"] = true
				} else {
					opts["AwsSDKLoadConfig"] = false
				}
			case "AWS_PROFILE":
				if kv[0] != "" {
					opts["AwsProfile"] = kv[1]
				} else {
					opts["AwsProfile"] = "default"
				}
			case "AWS_SHARED_CONFIG_ENABLED":
				if kv[0] == "1" || strings.ToLower(kv[1]) == "true" {
					opts["AwsSharedConfigEnabled"] = true
				} else {
					opts["AwsSharedConfigEnabled"] = false
				}
			case "AWS_BUCKET":
				opts["AwsBucket"] = kv[1]
			}
		}
	}
	return opts
}

// S3Configure is a function that configures a storage.Store for use with AWS S3
func S3Configure(store *Store) (*Store, error) {
	var (
		awsSDKLoadConfig       bool
		awsSharedConfigEnabled bool
		awsProfile             string

		sess *session.Session
		err  error
	)

	// Set storage type to S3
	store.Type = S3

	if val, ok := store.Config["AwsSDKLoadConfig"]; ok == true {
		awsSDKLoadConfig = val.(bool)
	}
	if val, ok := store.Config["AwsSharedConfigEnabled"]; ok == true {
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
		if val, ok := store.Config["AwsRegion"]; ok == true {
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
	store.Config["session"] = sess

	s3Svc := s3.New(sess)
	store.Config["s3Service"] = s3Svc

	// Basic Ops
	store.Create = func(fname string, rd io.Reader) error {
		return S3Create(store, fname, rd)
	}
	store.Read = func(fname string) ([]byte, error) {
		return S3Read(store, fname)
	}
	store.Update = func(fname string, rd io.Reader) error {
		// NOTE: Create and Update and the same in S3, Update overwrites the existing object
		return S3Create(store, fname, rd)
	}
	store.Delete = func(fname string) error {
		return S3Remove(store, fname)
	}

	// Extra ops for compatibilty with os.* and ioutil.*
	store.Stat = func(fname string) (os.FileInfo, error) {
		return S3Stat(store, fname)
	}
	store.Mkdir = func(name string, perm os.FileMode) error {
		//NOTE: S3 lacks the concept of directories, the fill path is the "Key" value info the bucket
		return nil
	}
	store.MkdirAll = func(path string, perm os.FileMode) error {
		//NOTE: S3 lacks the concept of directories, the fill path is the "Key" value info the bucket
		return nil
	}
	store.Remove = func(fname string) error {
		return S3Remove(store, fname)
	}
	store.RemoveAll = func(fname string) error {
		return S3RemoveAll(store, fname)
	}
	store.ReadFile = func(fname string) ([]byte, error) {
		return S3Read(store, fname)
	}
	store.WriteFile = func(fname string, data []byte, perm os.FileMode) error {
		return S3Create(store, fname, bytes.NewBuffer(data))
	}
	return store, nil
}

// S3Stat takes a file name and returns a FileInfo and error value
func S3Stat(s *Store, fname string) (os.FileInfo, error) {
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
// associated with the Store initialized.
func S3Create(s *Store, fname string, rd io.Reader) error {
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
func S3Read(s *Store, fname string) ([]byte, error) {
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
func S3Remove(s *Store, fname string) error {
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
func S3RemoveAll(s *Store, prefixName string) error {
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
				return err
			}
			cnt = len(res.Contents)
		}
		return nil
	}
	return fmt.Errorf("s3Service not configured")
}

// Init returns a *Store structure that points to configuration info (e.g. S3 credentials)
// and basic CRUD functions associated with the Store's storage type.
func Init(storeType int, options map[string]interface{}) (*Store, error) {
	store := new(Store)
	store.Type = UNSUPPORTED
	// Copy the values into store.Config so they can travel with the struct
	store.Config = map[string]interface{}{}
	for key, val := range options {
		store.Config[key] = val
	}
	switch storeType {
	case FS:
		return FSConfigure(store)
	case S3:
		return S3Configure(store)
	default:
		return store, fmt.Errorf("storeType not supported")
	}
}

// GetDefaultStore tries to guess the storage type based on environment settings
// if it can't is assumes storage.FS type.
func GetDefaultStore() *Store {
	opts := map[string]interface{}{}
	sType := FS
	for _, env := range os.Environ() {
		if strings.Contains(env, "=") {
			kv := strings.SplitN(env, "=", 2)
			if len(kv) == 2 {
				k, v := kv[0], kv[1]
				opts[k] = v
				if strings.HasPrefix(k, "AWS_") == true {
					sType = S3
				}
			}
		}
	}
	if sType == S3 {
		if s := os.Getenv("AWS_BUCKET"); s != "" {
			opts["AwsBucket"] = s
		}
		if s := os.Getenv("AWS_SDK_LOAD_CONFIG"); s == "1" || strings.ToLower(s) == "true" {
			opts["AwsSDKLoadConfig"] = true
			opts["AwsSharedConfigEnabled"] = true
			if t := os.Getenv("AWS_PROFILE"); t != "" {
				opts["AwsProfile"] = t
			} else {
				opts["AwsProfile"] = "default"
			}
		}
	}

	store, _ := Init(sType, opts)
	return store
}

// WriteAfter writes a file after running apply a filter function to its' file pointer
// E.g. composing a tarball before uploading results to S3
func (store *Store) WriteAfter(finalPath string, processor func(fp *os.File) error) error {
	// Open temp file as file point
	tmp, err := ioutil.TempFile(os.TempDir(), path.Base(finalPath))
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	tmpName := tmp.Name()

	// Envoke processor function
	err = processor(tmp)
	if err != nil {
		return err
	}
	err = tmp.Close()
	if err != nil {
		return err
	}

	// Store the results
	if store.Type == S3 {
		// if store.Type == S3, upload temp filename with fname
		if val, ok := store.Config["s3Service"]; ok == true {
			s3Svc := val.(s3iface.S3API)
			if _, ok := store.Config["AwsBucket"]; ok == false {
				return fmt.Errorf("Bucket not defined for %s", finalPath)
			}
			bucketName := store.Config["AwsBucket"].(string)

			rd, err := os.Open(tmpName)
			if err != nil {
				return err
			}
			defer rd.Close()

			upParams := &s3manager.UploadInput{
				Bucket: &bucketName,
				Key:    &finalPath,
				Body:   rd,
			}
			uploader := s3manager.NewUploaderWithClient(s3Svc)
			_, err = uploader.Upload(upParams)
			if err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf("s3Service not configured")
	}
	return os.Rename(tmpName, finalPath)
}
