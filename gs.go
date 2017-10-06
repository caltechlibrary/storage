//
// gs.go adds gs:// (Google Cloud Storage) support to storage.go
//
package storage

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

// GSConfigure is a function that configures a storage.Store for use with Google Cloud Storage
func GSConfigure(store *Store) (*Store, error) {
	// Set storage type to GS
	store.Type = GS

	// Initialization needed by Google Cloud Storage
	return nil, fmt.Errorf("DEBUG initialization not implemented!")

	// Basic Ops
	store.Create = func(fname string, rd io.Reader) error {
		return GSCreate(store, fname, rd)
	}
	store.Read = func(fname string) ([]byte, error) {
		return GSRead(store, fname)
	}
	store.Update = func(fname string, rd io.Reader) error {
		// NOTE: Create and Update and the same in GS, Update overwrites the existing object
		return GSCreate(store, fname, rd)
	}
	store.Delete = func(fname string) error {
		return GSRemove(store, fname)
	}

	// Extra ops for compatibilty with os.* and ioutil.*
	store.Stat = func(fname string) (os.FileInfo, error) {
		return GSStat(store, fname)
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
		return GSRemove(store, fname)
	}
	store.RemoveAll = func(fname string) error {
		return GSRemoveAll(store, fname)
	}
	store.ReadFile = func(fname string) ([]byte, error) {
		return GSRead(store, fname)
	}
	store.WriteFile = func(fname string, data []byte, perm os.FileMode) error {
		return GSCreate(store, fname, bytes.NewBuffer(data))
	}
	return store, nil
}

// GSCreate takes a full path and a byte array of content and writes it to the bucket
// associated with the Store initialized.
func GSCreate(s *Store, fname string, rd io.Reader) error {
	return fmt.Errorf("GSCreate() not implemented")
}

// GSRead takes a full path and returns a byte array and error from the bucket read
func GSRead(s *Store, fname string) ([]byte, error) {
	return nil, fmt.Errorf("GSRead() not implemented")
}

// GSRemove takes a full path and returns an error if delete not successful
func GSRemove(s *Store, fname string) error {
	return fmt.Errorf("GSRemove() not implemented")
}

// GSRemoveAll takes a path prefix and delete matching items (up to 1000) and returns an error if delete not successful
func GSRemoveAll(s *Store, prefixName string) error {
	return fmt.Errorf("GSRemoveAll() not implemented")
}

// GSStat takes a file name and returns a FileInfo and error value
func GSStat(s *Store, fname string) (os.FileInfo, error) {
	return nil, fmt.Errorf("GSStat() not implemented")
}
