//
// storage package wraps both local disc and S3 storage with CRUD operations and common os.*, ioutil.* functions.
//
// @author R. S. Doiel, <rsdoiel@library.caltech.edu>
//
// Copyright (c) 2020, Caltech
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
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
)

func TestFS(t *testing.T) {
	store, err := Init(FS, nil)
	if err != nil {
		t.Errorf("Init() failed, %s", err)
		t.FailNow()
	}

	// Create a directories  if needed
	err = store.Mkdir("testdata", 0775)
	if err != nil {
		t.Errorf("Can't create testdata directory, %s", err)
		t.FailNow()
	}
	err = store.MkdirAll("testdata/subdir1/subdir2", 0775)
	if err != nil {
		t.Errorf("Can't create testdata/subdir1/subdir2 directory, %s", err)
		t.FailNow()
	}

	fname := path.Join("testdata", "helloworld.txt")
	helloworld := []byte(`Hello World!!!!`)
	err = store.Create(fname, bytes.NewReader(helloworld))
	if err != nil {
		t.Errorf("Create error for %s, %s", fname, err)
		t.FailNow()
	}

	// Stat for FS
	fInfo, err := store.Stat(fname)
	if err != nil {
		t.Errorf("Stat error for %s, %s", fname, err)
		t.FailNow()
	}
	if fInfo == nil {
		t.Errorf("Stat missing fInfo object %ss", fname)
		t.FailNow()
	}
	if fInfo.Name() != path.Base(fname) {
		t.Errorf("Expected %s, got %s", path.Base(fname), fInfo.Name())
	}

	buf, err := store.Read(fname)
	if err != nil {
		t.Errorf("Read error for %s, %s", fname, err)
		t.FailNow()
	}
	if bytes.Compare(buf, helloworld) != 0 {
		t.Errorf("Expected %q, got %q", helloworld, buf)
		t.FailNow()
	}
	helloworld = []byte("Hello World.")
	err = store.Update(fname, bytes.NewReader(helloworld))
	if err != nil {
		t.Errorf("Update error for %s, %s", fname, err)
		t.FailNow()
	}
	// Re-read the data we just wrote out
	buf, err = store.Read(fname)
	if err != nil {
		t.Errorf("Read error for %s, %s after update", fname, err)
		t.FailNow()
	}
	if bytes.Compare(buf, helloworld) != 0 {
		t.Errorf("Expected %q, got %q after update", helloworld, buf)
		t.FailNow()
	}
	// Re-read the data we just wrote out
	buf, err = store.ReadFile(fname)
	if err != nil {
		t.Errorf("ReadFile error for %s, %s after update", fname, err)
		t.FailNow()
	}
	if bytes.Compare(buf, helloworld) != 0 {
		t.Errorf("Expected %q, got %q after update", helloworld, buf)
		t.FailNow()
	}

	// Write file out again
	data := []byte("Hi There")
	err = store.WriteFile(fname, data, 0664)
	if err != nil {
		t.Errorf("WriteFile error for %s, %s", fname, err)
		t.FailNow()
	}
	buf, err = store.ReadFile(fname)
	if err != nil {
		t.Errorf("ReadFile error for %s, %s after update", fname, err)
		t.FailNow()
	}
	if bytes.Compare(data, buf) != 0 {
		t.Errorf("Expected %q, got %q after update", data, buf)
		t.FailNow()
	}

	err = store.Delete(fname)
	if err != nil {
		t.Errorf("Delete error for %s, %s", fname, err)
		t.FailNow()
	}
	// Cleanup if successful so far
	err = store.Remove("testdata/subdir1/subdir2")
	if err != nil {
		t.Errorf("Could not remove testdata/subdir1/subdir2s, %s", err)
	}
	err = store.RemoveAll("testdata")
	if err != nil {
		t.Errorf("Could not remove testdata and it's children, %s", err)
	}
}

func TestGetDefaultStore(t *testing.T) {
	// Clear the environment for test.
	opts := map[string]string{}
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, "AWS_") == true {
			kv := strings.SplitN(env, "=", 2)
			opts[kv[0]] = kv[1]
			os.Unsetenv(kv[0])
		}
	}
	store, err := GetDefaultStore()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if store.Type == UNSUPPORTED {
		t.Errorf("Expected FS type, got UNSUPPORTED")
		t.FailNow()
	}
}

func TestWriteFilter(t *testing.T) {
	store, err := Init(FS, nil)
	if err != nil {
		t.Errorf("Init failed, %s", err)
		t.FailNow()
	}
	start := 0
	finish := 9
	store.Mkdir("testdata", 0775)
	err = store.WriteFilter("testdata/after-test.txt", func(fp *os.File) error {
		for i := start; i <= finish; i++ {
			fmt.Fprintf(fp, "%d\n", i)
		}
		return nil
	})
	if err != nil {
		t.Errorf("WriteFilter, %s", err)
		t.FailNow()
	}
	data, err := store.ReadFile("testdata/after-test.txt")
	if err != nil {
		t.Errorf("ReadFile %s", err)
		t.FailNow()
	}
	ok := true
	for i, line := range strings.Split(string(data), "\n") {
		if i <= finish && fmt.Sprintf("%d", i) != line {
			t.Errorf("mismatch at line %d, expected %d, got %s", i, i, line)
			ok = false
		}
	}
	if ok {
		store.RemoveAll("testdata")
	}
}

func TestReadDir(t *testing.T) {
	store, err := Init(FS, nil)
	if err != nil {
		t.Errorf("failed to init store, %s", err)
		t.FailNow()
	}
	expectedDir, err := ioutil.ReadDir(".")
	if err != nil {
		t.Errorf("Can't read ./ for testing, %s", err)
		t.FailNow()
	}
	testDir, err := store.ReadDir(".")
	if err != nil {
		t.Errorf("store.ReadDir(%q), %s", "./", err)
		t.FailNow()
	}
	if len(expectedDir) != len(testDir) {
		t.Errorf("Mismatch in test (%d) and expected (%d) directory counts", len(expectedDir), len(testDir))
	}
	for i, expected := range expectedDir {
		if i < len(testDir) {
			if expected.Name() != testDir[i].Name() {
				t.Errorf("expected (%d) %q, got %q", i, expected.Name(), testDir[i].Name())
			}
		}
	}
}

// Check to make sure StorageType is detectable from provided paths
func TestStorageType(t *testing.T) {
	m := map[string]int{
		"/my/stuff":              FS,
		"stuff":                  FS,
		"foo.txt":                FS,
		"s3://example.edu/stuff": UNSUPPORTED,
		"gs://example.edu/stuff": UNSUPPORTED,
		"eworiwer://example.io/": UNSUPPORTED,
		"https://example.io":     UNSUPPORTED,
		"http://erwerew":         UNSUPPORTED,
		"gopher://ewreweww":      UNSUPPORTED,
	}
	for p, expected := range m {
		if r := StorageType(p); r != expected {
			switch expected {
			case UNSUPPORTED:
				t.Errorf("expected UNSUPPORTED (%d), got %d", expected, r)
			default:
				t.Errorf("expected %d, got %d", expected, r)
			}
		}
	}
}

func TestFindAndExistence(t *testing.T) {
	// Check to see if we can find README.md in the list of files
	store, err := GetStore(".")
	if err != nil {
		t.Errorf("Expected to current working directory, %s", err)
		t.FailNow()
	}
	if store.IsDir(".") == false {
		t.Errorf("expected true, got false, IsDir(\".\")")
		t.FailNow()
	}
	if store.IsFile(".") == true {
		t.Errorf("expected false, got true, IsFile(\".\")")
		t.FailNow()
	}
	files, err := store.FindByExt(".", ".md")
	if err != nil {
		t.Errorf("Expected a list of files from '.', %s", err)
		t.FailNow()
	}
	foundIt := false
	for _, fname := range files {
		if fname == "README.md" {
			foundIt = true
			break
		}
	}
	if foundIt == false {
		t.Errorf("Expected to find README.md in file list, %+v", files)
		t.FailNow()
	}
}

func TestLocation(t *testing.T) {
	workPath := "src/stuff/data"
	store, err := Init(FS, nil)
	if err != nil {
		t.Errorf("Init() failed, %s", err)
		t.FailNow()
	}
	loc, err := store.Location(workPath)
	if err != nil {
		t.Errorf("Location() failed, %s", err)
		t.FailNow()
	}
	if workPath != loc {
		t.Errorf("expected %q, got %q", workPath, loc)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}
