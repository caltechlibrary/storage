package storage

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
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
	err = site.Create(fname, bytes.NewReader(helloworld))
	if err != nil {
		t.Errorf("Create error for %s, %s", fname, err)
		t.FailNow()
	}

	// Stat for FS
	fInfo, err := site.Stat(fname)
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
	// Cleanup if successful so far
	os.RemoveAll("testdata")
}

func TestS3(t *testing.T) {
	if testS3 == true {
		options := map[string]interface{}{}

		if s := os.Getenv("AWS_BUCKET"); s != "" {
			options["AwsBucket"] = s
		} else {
			options["AwsBucket"] = "test"
		}

		if s := os.Getenv("AWS_SDK_LOAD_CONFIG"); s == "1" || strings.ToLower(s) == "true" {
			options["AwsSDKLoadConfig"] = true
			options["AwsSharedConfigEnabled"] = true
			if t := os.Getenv("AWS_PROFILE"); t != "" {
				options["AwsProfile"] = t
			} else {
				options["AwsProfile"] = "default"
			}
		}

		site, err := Init(S3, options)
		if err != nil {
			t.Errorf("%s", err)
			t.FailNow()
		}
		if site == nil {
			t.Errorf("site was nil")
			t.FailNow()
		}

		fname := `testdata/helloworld.txt`
		expected := []byte(`Hello World!!!`)
		err = site.Create(fname, bytes.NewReader(expected))
		if err != nil {
			t.Errorf("%s", err)
			t.FailNow()
		}

		// Stat for S3
		fInfo, err := site.Stat(fname)
		if err != nil {
			t.Errorf("Stat error for %s, %s", fname, err)
			t.FailNow()
		}
		if fInfo == nil {
			t.Errorf("Stat missing fInfo object %ss", fname)
			t.FailNow()
		}
		if fInfo.Name() != path.Base(fname) {
			t.Errorf("expected %s, got %s", path.Base(fname), fInfo.Name())
		}
		if fInfo.Size() != int64(len(expected)) {
			t.Errorf("expected %d, got %d", int64(len(expected)), fInfo.Size())
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
	flag.BoolVar(&testS3, "s3", false, "Run S3 storageType tests")
	flag.Parse()
	os.Exit(m.Run())
}
