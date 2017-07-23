# go-nosql

# Description

**go-nosql** is a simplified wrapper for NoSQL databases that provides a common API interface.  As it is a simplified wrapper, it cannot cover all database-specific features.  Each database wrapped implements the `NOSQLBackend` interface.  [DynamoDB](https://aws.amazon.com/dynamodb/) and [MongoDB](https://www.mongodb.com/) are currently supported.

Struct `Table` is used when calling `CreateTables` as DynamoDB requires defining [Global Secondary Indexes](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html).  Each attribute is assumed to be a string.

`Table === MongoDB Collection` for the purposes of this API.

```
type Table struct {
  Name string
  Indexes []string
}
```

# Installation

```
go get github.com/spatialcurrent/go-nosql
```

# Usage

**Import**

```
import (
  "github.com/spatialcurrent/go-nosql/nosql"
)
```

**Local DynamoDB**

```
backend = &nosql.BackendDynamoDB{}
err := backend.Connect(map[string]string{
  "AWSDefaultRegion": "us-west-1",
  "StorefrontDynamoDBUrl": "http://localhost:8082",
})
```

**Remote DynamoDB**

```
backend = &nosql.BackendDynamoDB{}
err := backend.Connect(map[string]string{
  "AWSAccessKeyId": "",
  "AWSSecretAccessKey": "",
  "AWSSessionToken": "",
  "AWSDefaultRegion": "us-west-1",
})
```

**MongoDB**

```
backend = &nosql.BackendMongoDB{}
err := backend.Connect(map[string]string{
  "DatabaseUri": "localhost",
  "DatabaseName": "main",
  "Limit": "1000",
})
```

**API**

See [Backend.go](https://github.com/spatialcurrent/go-nosql/blob/master/nosql/Backend.go) for the public APIs for each backend.

# Contributing

[Spatial Current, Inc.](https://spatialcurrent.io) is currently accepting pull requests for this repository.  We'd love to have your contributions!  Please see [Contributing.md](https://github.com/spatialcurrent/go-nosql/blob/master/CONTRIBUTING.md) for how to get started.

# License

This work is distributed under the **MIT License**.  See **LICENSE** file.
