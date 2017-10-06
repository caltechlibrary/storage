//
// gs.go adds gs:// (Google Cloud Storage) support to storage.go
//
package storage

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
)

// GSConfigure is a function that configures a storage.Store for use with Google Cloud Storage
func gsConfigure(store *Store) (*Store, error) {
	// Set storage type to GS
	store.Type = GS

	// Initialization needed by Google Cloud Storage
	return nil, fmt.Errorf("DEBUG initialization not implemented!")

	// Basic Ops
	store.Create = func(fname string, rd io.Reader) error {
		return gsCreate(store, fname, rd)
	}
	store.Read = func(fname string) ([]byte, error) {
		return gsRead(store, fname)
	}
	store.Update = func(fname string, rd io.Reader) error {
		// NOTE: Create and Update and the same in GS, Update overwrites the existing object
		return gsCreate(store, fname, rd)
	}
	store.Delete = func(fname string) error {
		return gsRemove(store, fname)
	}

	// Extra ops for compatibilty with os.* and ioutil.*
	store.Stat = func(fname string) (os.FileInfo, error) {
		return gsStat(store, fname)
	}
	store.Mkdir = func(name string, perm os.FileMode) error {
		//NOTE: GS lacks the concept of directories, the fill path is the "Key" value info the bucket
		return nil
	}
	store.MkdirAll = func(path string, perm os.FileMode) error {
		//NOTE: GS lacks the concept of directories, the fill path is the "Key" value info the bucket
		return nil
	}
	store.Remove = func(fname string) error {
		return gsRemove(store, fname)
	}
	store.RemoveAll = func(fname string) error {
		return gsRemoveAll(store, fname)
	}
	store.ReadFile = func(fname string) ([]byte, error) {
		return gsRead(store, fname)
	}
	store.WriteFile = func(fname string, data []byte, perm os.FileMode) error {
		return gsCreate(store, fname, bytes.NewBuffer(data))
	}

	// Extended options for datatools and dataset

	// WriteFilter writes a file after running apply a filter function to its' file pointer
	// E.g. composing a tarball before uploading results to S3
	store.WriteFilter = func(finalPath string, processor func(*os.File) error) error {
		// Open temp file as file point
		tmp, err := ioutil.TempFile(os.TempDir(), path.Base(finalPath))
		if err != nil {
			return err
		}
		tmpName := tmp.Name()
		defer os.Remove(tmpName)

		// Envoke processor function
		err = processor(tmp)
		if err != nil {
			return err
		}
		err = tmp.Close()
		if err != nil {
			return err
		}

		// OK now we're ready to upload temp filename to final path
		return fmt.Errorf("WriteFilter for gs:// not fully implemented")
	}

	// Now the store is setup and we're ready to return
	return store, nil
}

// GSCreate takes a full path and a byte array of content and writes it to the bucket
// associated with the Store initialized.
func gsCreate(s *Store, fname string, rd io.Reader) error {
	return fmt.Errorf("GSCreate() not implemented")
}

// GSRead takes a full path and returns a byte array and error from the bucket read
func gsRead(s *Store, fname string) ([]byte, error) {
	return nil, fmt.Errorf("GSRead() not implemented")
}

// GSRemove takes a full path and returns an error if delete not successful
func gsRemove(s *Store, fname string) error {
	return fmt.Errorf("GSRemove() not implemented")
}

// GSRemoveAll takes a path prefix and delete matching items (up to 1000) and returns an error if delete not successful
func gsRemoveAll(s *Store, prefixName string) error {
	return fmt.Errorf("GSRemoveAll() not implemented")
}

// GSStat takes a file name and returns a FileInfo and error value
func gsStat(s *Store, fname string) (os.FileInfo, error) {
	return nil, fmt.Errorf("GSStat() not implemented")
}
