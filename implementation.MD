# Implementation

## Go sql library

The [go sql library](https://pkg.go.dev/database/sql) and the [go sql driver library](https://pkg.go.dev/database/sql/driver) 

## MonetDB driver

The previous version, v1.1.0, implemented the Go v1.7 library interfaces. The latest version, v2.0.0, implements the latest database interfaces (go 1.21.5). This provides more functionality, specifically the column types and supporting the context interface.

### Go module major version

It turns out that the go module system has a specific way of implementing a new major version of a module. This is explained in [this](https://go.dev/blog/v2-go-modules#TOC_4.) blog post. This means the the published version v2.0.0 is completely unusable. Unfortunately, when you fix the repository as suggested, the old version of the module becomes completely unusable. There is no obvious fix for this, so for now we accept this. This means that the code in this repository only works for major version v2. We also needed to upgrade the required go version to 1.18, because we needed the go workspace functionality in order to be able to run tests for the v2 version

### Mapi

[Mapi](https://www.monetdb.org/documentation-Jun2023/user-guide/client-interfaces/libraries-drivers/mapi-library/) is the API that provides the communication protocol with the MonetDB database. To understand the details of the mapi protocol implementation in this library, check the documentation in the [PHP](https://github.com/MonetDB/MonetDB-PHP/tree/master/protocol_doc) driver.

#### Type aliases

Besides a number of [column types](https://www.monetdb.org/documentation/user-guide/sql-manual/data-types/base-types/), MonetDB also defines type aliases. We don't need to implement anything specific to support a type alias. The code that converts between a MonetDB and a Go type uses the actual column type that is returned by Mapi protocol. The type aliases are only used in the SQL query.

### Refactoring

We will create resultset, resultset schema and resultset metadata types. The code in the current previous implementation, for example the description type and the statement.storeResult function, will be moved to this new go source file. We will move all monetdb specific code out of the implementation of the sql driver interfaces. With the resultset types implemented, it will be relatively easy to implement the column type interfaces.

We will also move the code for handling the connection config out of the driver.go source file. We want to separate the MonetDB specific implementation details from the more generic sql library code as much as possible. This will make the it easier to understand the implementation and easier to implement new interfaces.

We will prefix the error messages with the package name. That way it is clear where the error message comes from, the driver implementation or the mapi library.

#### Todo
- [X] in driver.go move parsedsn function call inside newConn
- [X] move tests from driver_test.go to new file after change to driver.open
- [X] move config type from driver.go
- [X] Conn struct doesn't need a config field
- [ ] set_autocommit (see: [pymonetdb](https://github.com/MonetDB/pymonetdb/blob/master/pymonetdb/sql/connections.py#L156C16-L156C16))
- [ ] change_replysize
- [ ] set_timezone
- [ ] set_uploader
- [ ] set_downloader
- [ ] Configure connection using socket
- [X] Implement fetching NextResultSet
- [X] Add type aliases
- [X] Add monetdb specific types, for example "uuid"

## driver package and sql package latest version

We need to add context everywhere. We need to implement the RowsColumnType interfaces. We can implement transaction isolation level.

### context library

The [go context library](https://pkg.go.dev/context)

In version 1, every statement was a prepared statement. This is not needed in many cases. In version 2 this is changed. The Stmt struct has a "isPreparedStatement" field. This is only set to true when a statement is generated with a "Prepare" function. There is an executeStmt function now, that can be used to execute a single query against the database. This is used for example for the commit and rollbacks of transactions.

### New interfaces

### Not implemented

- The [Out](https://pkg.go.dev/database/sql#Out) interface is not implemented because MonetDB stored procedures do not support (IN)OUT parameters at the moment.
- The [documentation](https://www.monetdb.org/documentation-Dec2023/user-guide/sql-manual/transactions/) mentions the TRANSACTION READ ONLY option, but it is not supported in the MonetDB server.
- The [Nullable](https://pkg.go.dev/database/sql#ColumnType.Nullable) function always returns false, because the Mapi protocol does not return information about the (NOT) NULL property of a column.
- The [NextResultSet](https://pkg.go.dev/database/sql#Rows.NextResultSet) function always returns false, because the current implementation of mapi.ResultSet.StoreResult function does not handle multiple resultsets.

### Testing

The [go testing library](https://pkg.go.dev/testing)

### Examples

The examples directory contains several scripts that demonstrate how the monetdb driver for the sql library should be used. They contain the same functionality as the integration tests, but in form that show howto implement certain patterns, instead of verifying the correct results.
