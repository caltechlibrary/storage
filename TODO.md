
# Action Items

## Next

+ [ ] Add a Join(), Dir(), Basename() for paths that are local as well as as remove
    + [ ] fs.go
    + [ ] gs.go
    + [ ] s3.go
+ [ ] Convert from vendor specific packages to go-cloud 
    + [ ] refactor fs.go
    + [ ] refactor gs.go
    + [ ] refactor s3.go
    + [ ] add support for Azure blob store
    + [ ] add support for in-memory store
    + [ ] add support for MySQL JSON store
    + [ ] Add support for ReadDir() (this should be refactored to use go-cloud package
        + [x] for FS
        + [x] for S3
        + [ ] for GS

## Someday, Maybe

+ [ ] CopyFile
+ [ ] add support for our NAS
+ [ ] add support for Dropbox
+ [ ] add support for Google Drive
+ [ ] add support for Box

## Completed

+ [x] Review https://github.com/sajari/storage, see how hard it would be to add S3 support or add support for similar systems in our storage
+ [x] Add support for Google Cloud Storage with gs:// URL prefix in config environment
+ [x] WriteFileAfter - create a temp file, apply a function on the file pointer, then move to final location (e.g. local FS or S3)
    + finalName (path to final distination)
    + processingFunc (function to recieve the file point, do work, close FP, and then envoke a rename/move to final location
+ [x] Stat
+ [x] Remove
+ [x] RemoveAll
+ [x] Mkdir
+ [x] MkdirAll
+ [x] ReadFile
+ [x] WriteFile

