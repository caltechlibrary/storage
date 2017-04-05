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
	testS3 string
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
	err = site.Create(fname, bytes.NewReader(helloworld))
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
	err = site.Update(fname, bytes.NewReader(helloworld))
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
	if testS3 != "" {
		options := map[string]interface{}{
			"Profile": testS3,
		}
		site, err := Init(S3, options)
		if err != nil {
			t.Errorf("%s", err)
			t.FailNow()
		}

		fname := `testdata/helloworld.txt`
		expected := []byte(`Hello World!!!`)
		err = site.Create(fname, bytes.NewReader(expected))
		if err != nil {
			t.Errorf("%s", err)
			t.FailNow()
		}
		result, err := site.Read(fname)
		if err != nil {
			t.Errorf("%s", err)
			t.FailNow()
		}
		if bytes.Compare(expected, result) != 0 {
			t.Errorf("expected %q, got %q", expected, result)
			t.FailNow()
		}
		expected = []byte(`Hello World.`)
		err = site.Update(fname, bytes.NewReader(expected))
		if err != nil {
			t.Errorf("%s", err)
			t.FailNow()
		}
		// Now read back the data and make sure it changed
		result, err = site.Read(fname)
		if err != nil {
			t.Errorf("%s", err)
			t.FailNow()
		}
		if bytes.Compare(expected, result) != 0 {
			t.Errorf("expected %q, got %q", expected, result)
			t.FailNow()
		}
		err = site.Delete(fname)
		if err != nil {
			t.Errorf("%s", err)
			t.FailNow()
		}
	} else {
		fmt.Println("Skipping TestS3")
	}
}

func TestMain(m *testing.M) {
	flag.StringVar(&testS3, "s3", "", "Run S3 storageType tests using matching profile")
	flag.Parse()
	os.Exit(m.Run())
}
