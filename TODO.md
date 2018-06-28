
# Action Items

## Next

+ [ ] Add support for Readdir()

## Someday, Maybe

+ [ ] CopyFile
+ [ ] add support for our NAS
+ [ ] add support for Dropbox
+ [ ] add support for Google Drive
+ [ ] add support for Box
+ [ ] add support for [Minio](https://minio.io/)

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


