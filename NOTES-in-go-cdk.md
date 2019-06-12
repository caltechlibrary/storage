
# Notes on Go CDK (go-cloud)

The [go-cloud](https://gocloud.dev/) now called Go CDK is a package would provides a common methods to access S3, Google Cloud and Azure blob storage services. It also supports local file system and in memory storage.

Additionally if we wrap our storage metaphor around database engines based on MySQL and Posgresql we could use the Go CDK to provide support for various types of key/value record look up whether it was in a blob store, a RDBMs or on local disc (where path is key and the data is the contents of the file).

The goal then of storage.go would be to provide the lightest wrapper around the go-cloud package while providing familiar coding interfaces like that found in path, os and ioutil. This should expand our capability while reducing our the total ammount of code directly maintained by Caltech Library staff.

Notible in the Go CDK docs is that the list funcs can return prefix paths as directories. This would better support walking the file tree in a manner similar to local disc.

It appears to eliviate the needs to import the whole Google and Amazon SDKs and their hundreds of packages while still accessing the same basic service models we use.

