//
// gocdk.go wraps Go Cloud Development Kit's blob package for use
// with our storage module.  It will allow us to drop individual wrappers
// e.g. fs.go, s3.go, gs.go while also picking up support for in-memory
// and Azure based blob storage.
//
// For docs see https://godoc.org/gocloud.dev/blob
//
package storage

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	// Go Cloud Development Kit
	"gocloud.dev/blob"
)

// cdkConfigure initialize a store to a local disc system type
func cdkConfigure(store *Store) (*Store, error) {
	store.Type = GO_CDK

	// Basic CRUD ops
	store.Create = func(fname string, rd io.Reader) error {
		return fmt.Errorf("store.Create() not implemented")
	}
	store.Read = func(fname string) ([]byte, error) {
		return fmt.Errorf("store.Read() not implemented")
	}
	store.Update = func(fname string, rd io.Reader) error {
		return fmt.Errorf("store.Update() not implemented")
	}
	store.Delete = func(fname string) error {
		return fmt.Errorf("store.Delete() not implemented")
	}

	// Extra ops for compatibility with os.* and ioutil.*
	store.Stat = func(fname string) (os.FileInfo, error) {
		return nil, fmt.Errorf("store.Stat() not implemented")
	}
	store.Mkdir = func(name string, perm os.FileMode) error {
		return fmt.Errorf("store.Mkdir() not implemented")
	}
	store.MkdirAll = func(path string, perm os.FileMode) error {
		return fmt.Errorf("store.MkdirAll() not implemented")
	}
	store.Remove = func(name string) error {
		return fmt.Errorf("store.Remove() not implemented")
	}
	store.RemoveAll = func(path string) error {
		return fmt.Errorf("store.RemoveAll() not implemented")
	}
	store.ReadFile = func(fname string) ([]byte, error) {
		return fmt.Errorf("store.ReadFile() not implemented")
	}
	store.WriteFile = func(fname string, data []byte, perm os.FileMode) error {
		return fmt.Errorf("store.WriteFile() not implemented")
	}
	store.ReadDir = func(fname string) ([]os.FileInfo, error) {
		return fmt.Errof("store.ReadDir() not implemented")
	}

	//
	// Add Path related funcs
	//
	store.Base = func(p string) string {
		return "" //FIXME: NOT Implemented, path.Base(p)
	}
	store.Clean = func(p string) string {
		return "" //FIXME: NOT Implemented, path.Clean(p)
	}
	store.Dir = func(p string) string {
		return "" //FIXME: NOT Implemented, path.Dir(p)
	}
	store.Ext = func(p string) string {
		return "" //FIXME: NOT Implemented, path.Ext(p)
	}
	store.IsAbs = func(p string) bool {
		return "" //FIXME: NOT Implemented, path.IsAbs(p)
	}
	store.Join = func(elem ...string) string {
		return "" //FIXME: NOT Implemented, path.Join(elem...)
	}
	store.Match = func(pattern string, name string) (matched bool, err error) {
		return false, fmt.Errorf("store.Match() not implemented") //FIXME
	}
	store.Split = func(p string) (dir, filename string) {
		return "" //FIXME: NOT Implemented, path.Split(p)
	}

	// Extended ops for datatools and dataset

	// WriteFilter writes a file after running/applying a filter function to its' file pointer
	// E.g. composing a tarball before storing
	store.WriteFilter = func(finalPath string, processor func(*os.File) error) error {
		return fmt.Errorf("store.WriteFillter() not implemented")
	}

	// Now the store is setup and we're ready to return
	return store, nil
}
