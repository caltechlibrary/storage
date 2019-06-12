
# Action Items

## Next

+ [ ] Convert from vendor specific packages to go-cloud 
    + [x] refactor fs.go (no change needed when adopting go-cloud)
    + [ ] refactor gs.go
    + [ ] refactor s3.go
    + [ ] add support for Azure blob store
    + [ ] add support for in-memory store
    + [ ] add support for MySQL JSON store
    + [ ] Add support for ReadDir() (this should be refactored to use go-cloud package
        + [x] for fs.go
        + [x] for s3.go
        + [ ] for gs.go

## Someday, Maybe

+ [ ] CopyFile
+ [ ] add support for our NAS
+ [ ] add support for Dropbox
+ [ ] add support for Google Drive
+ [ ] add support for Box

## Completed

+ [x] Add a Join(), Dir(), Basename() for paths that are local as well as as remove
    + [x] fs.go
    + [x] gs.go
    + [x] s3.go
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

