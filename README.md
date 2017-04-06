
# storage

This go package wraps storage options used by Caltech Library in standard CRUD operations
for local file system and AWS S3.

If you have your local AWS environment setup then you'll probably only need to set two
environment variables for this to work just like CRUD to the local file system.

```shell
    export AWS_SDK_LOAD_CONFIG=1
    export AWS_BUCKET=bucket.example.edu
```


