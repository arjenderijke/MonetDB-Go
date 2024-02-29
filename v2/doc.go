/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*

Package monetdb contains a database driver for MonetDB.

Please check the project's GitHub page for more complete documentation -
https://github.com/MonetDB/MonetDB-Go

# Usage

There are two options to setup a connection to the database. First is to
use the Open function:

``` go
	import (
		"database/sql"

		_ "github.com/MonetDB/MonetDB-Go/v2"
	)

	func main() {
		db, err := sql.Open("monetdb", DSN)

		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
	}
```

Use the following format for the Data Source Name (DSN) to make connection
to the MonetDB server.

    [username[:password]@]hostname[:port]/database

If the port is not specified, then the default port 50000 will be used.

The second option is to use the Connector, which allows for additional configuration options:

``` go
	func main() {
		connector, err := monetdb.NewConnector("monetdb:monetdb@localhost:50000/monetdb")
		if err != nil {
			log.Fatal(err)
	    }

		db := sql.OpenDB(connector)

	    defer db.Close()
    ?
```

## Options

The connector supports the following options:
- Sizeheader (default: enable) : Return the precision and scale of a decimal column
- ReplySize (default: 100): Maximum number of rows that will be returned in the resultset
- Autocommit (default: enable): Commit each individual sql statement
- Timezone (default: local timezone): Set the timezone of the database

You can add the required options when creating the new connector:
``` go
	func main() {
		connector, err := monetdb.NewConnector("monetdb:monetdb@localhost:50000/monetdb", monetdb.SizeHeaderOption(true))
	}
```
*/
package monetdb
