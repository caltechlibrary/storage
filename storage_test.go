package storage

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"
)

var (
	testS3 bool
)

func TestFS(t *testing.T) {
	site, err := Init(FS, nil)
	if err != nil {
		t.Errorf("Init() failed, %s", err)
		t.FailNow()
	}
	defer site.Close()

	// Create a file T if needed
	os.Mkdir("testdata", 0775)
	fname := path.Join("testdata", "helloworld.txt")
	helloworld := []byte(`Hello World!!!!`)
	err = site.Create(fname, helloworld)
	if err != nil {
		t.Errorf("Create error for %s, %s", fname, err)
		t.FailNow()
	}
	buf, err := site.Read(fname)
	if err != nil {
		t.Errorf("Read error for %s, %s", fname, err)
		t.FailNow()
	}
	if bytes.Compare(buf, helloworld) != 0 {
		t.Errorf("Expected %q, got %q", helloworld, buf)
		t.FailNow()
	}
	helloworld = []byte("Hello World.")
	err = site.Update(fname, helloworld)
	if err != nil {
		t.Errorf("Update error for %s, %s", fname, err)
		t.FailNow()
	}
	// Re-read the data we just wrote out
	buf, err = site.Read(fname)
	if err != nil {
		t.Errorf("Read error for %s, %s after update", fname, err)
		t.FailNow()
	}
	if bytes.Compare(buf, helloworld) != 0 {
		t.Errorf("Expected %q, got %q after update", helloworld, buf)
		t.FailNow()
	}
	err = site.Delete(fname)
	if err != nil {
		t.Errorf("Delete error for %s, %s", fname, err)
		t.FailNow()
	}
}

func TestS3(t *testing.T) {
	if testS3 == true {

		t.Errorf("TestS3() not implemented.")
		t.FailNow()
	} else {
		fmt.Println("Skipping TestS3")
	}
}

func TestMain(m *testing.M) {
	flag.BoolVar(&testS3, "s3", false, "Run S3 storageType tests")
	flag.Parse()
	os.Exit(m.Run())
}
