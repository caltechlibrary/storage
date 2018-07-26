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
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
)

const (
	// Version of package
	Version = `v0.0.6`

	// UNSUPPORTED is used if Init fails the and a non-nil Store struck gets returned.
	UNSUPPORTED = iota
	// FS local file system
	FS
	// S3 remote storage via AWS S3
	S3
	// GS remote storage via Google Cloud Storage
	GS
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
	ReadDir   func(string) ([]os.FileInfo, error)

	// Extended operations for datatools and dataset
	// Writefilter takes a final path and a processing function which accepts the temp pointer
	WriteFilter func(string, func(*os.File) error) error
}

// EnvToOptions given an environment map envvars to their option.
func EnvToOptions(env []string) map[string]interface{} {
	opts := map[string]interface{}{}
	for _, stmt := range env {
		switch {
		case strings.HasPrefix(stmt, "AWS_") && strings.Contains(stmt, "="):
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
		case (strings.HasPrefix(stmt, "GOOGLE_")) && strings.Contains(stmt, "="):
			kv := strings.SplitN(stmt, "=", 2)
			switch kv[0] {
			case "GOOGLE_BUCKET":
				opts["GoogleBucket"] = kv[1]
			}
		}
	}
	return opts
}

// Init returns a Store struct and error based on the provided
// type and options.
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
		return fsConfigure(store)
	case S3:
		return s3Configure(store)
	case GS:
		return gsConfigure(store)
	default:
		return store, fmt.Errorf("storeType not supported")
	}
}

// StorageType takes a path or URL and makes a guess
// as to which storage system is being referenced.
// Returns the integer value of the const identifying the type.
func StorageType(p string) int {
	s := strings.ToLower(p)
	if strings.Contains(p, "://") {
		switch {
		case strings.HasPrefix(s, "s3://"):
			return S3
		case strings.HasPrefix(s, "gs://"):
			return GS
		default:
			return UNSUPPORTED
		}
	}
	return FS
}

// GetDefaultStore returns a new Store based on environment settings.
// If no environment settings found then the storage type
// is assumed to be FS.
//
// Returns a new Store and error
func GetDefaultStore() (*Store, error) {
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
	//FIXME: Shouldn't we be calling individual typed default functions per sType? (e.g. in fs.go, s3.go, gs.go)
	switch sType {
	case S3:
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
	case GS:
		if s := os.Getenv("GOOGLE_PROJECT_ID"); s != "" {
			opts["GoogleProjectID"] = s
		}
		if s := os.Getenv("GOOGLE_BUCKECT"); s != "" {
			opts["GoogleBucket"] = s
		}
	}
	store, err := Init(sType, opts)
	return store, err
}

// GetStore creates a new Store struct based on the path provided. Unlike
// Init it derives the storage type from the path provided and populated options
// based on that path.
//
// Returns a new Store struct and error
func GetStore(name string) (*Store, error) {
	// Get store type
	sType := StorageType(name)
	opts := map[string]interface{}{}

	// Init the store based on storage type detected.
	switch sType {
	case S3:
		// NOTE: Attempting to overwrite the lack of an environment variable AWS_SDK_LOAD_CONFIG=1
		if os.Getenv("AWS_SDK_LOAD_CONFIG") == "" {
			os.Setenv("AWS_SDK_LOAD_CONFIG", "1")
		}
		u, _ := url.Parse(name)
		opts := EnvToOptions(os.Environ())
		opts["AwsBucket"] = u.Host
	case GS:
		u, _ := url.Parse(name)
		opts := EnvToOptions(os.Environ())
		opts["GoogleBucket"] = u.Host
	}
	store, err := Init(sType, opts)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// FindByExt retrieves a list of documents with the
// matching extension from the folder/directory indicated by
// path. It is non-recursive and only scans the provided path
// for the file extension.
//
// The extension you're searching for should include the dot (e.g. .json)
func (store *Store) FindByExt(p string, ext string) ([]string, error) {
	var docs []string

	dirInfo, err := store.ReadDir(p)
	if err != nil {
		return docs, err
	}
	for _, item := range dirInfo {
		if item.IsDir() == false {
			fname := item.Name()
			if suffix := path.Ext(fname); suffix == ext {
				docs = append(docs, fname)
			}
		}
	}
	return docs, nil
}

// IsFile returns true if the result of checking Stat
// on the path exists and is not a directory
func (store *Store) IsFile(p string) bool {
	info, err := store.Stat(p)
	if err != nil {
		return false
	}
	if info.IsDir() {
		return false
	}
	return true
}

// IsDir returns true if the result of checking Stat
// on path exists and is a directory
func (store *Store) IsDir(p string) bool {
	info, err := store.Stat(p)
	if os.IsNotExist(err) {
		return false
	}
	if info.IsDir() {
		return true
	}
	return false
}
