/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package monetdb

import (
	"database/sql"
	"testing"
	"time"
)

func TestTimezoneIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	connector, err := NewConnector("monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	if connector == nil {
		t.Fatal("Connector is not created")
	}
	db := sql.OpenDB(connector)
	if db == nil {
		t.Fatal("DB is not created")
	}
	defer db.Close()
	if pingErr := db.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}

	timezone1, err := time.LoadLocation("Europe/Amsterdam")
	if err != nil {
		t.Fatal("unable to define timezone")
	}
	connector1, err := NewConnector("monetdb:monetdb@localhost:50000/monetdb", TimezoneOption(timezone1))
	if err != nil {
		t.Fatal(err)
	}
	if connector1 == nil {
		t.Fatal("Connector is not created")
	}
	db1 := sql.OpenDB(connector1)
	if db1 == nil {
		t.Fatal("DB is not created")
	}
	defer db1.Close()
	if pingErr := db1.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}

	t.Run("Exec create table", func(t *testing.T) {
		result, err := db.Exec("create table test1 ( name decimal, value decimal(10, 5))")
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("query did not return a result object")
		}
		rId, err := result.LastInsertId()
		if err != nil {
			t.Error("Could not get id from result")
		}
		if rId != 0 {
			t.Errorf("Unexpected id %d", rId)
		}
		nRows, err := result.RowsAffected()
		if err != nil {
			t.Error("Could not get number of rows from result")
		}
		if nRows != 0 {
			t.Errorf("Unexpected number of rows %d", nRows)
		}
	})

	t.Run("Exec insert row", func(t *testing.T) {
		result, err := db.Exec("insert into test1 values ( 1.2345, 67.890 )")
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("query did not return a result object")
		}
		rId, err := result.LastInsertId()
		if err != nil {
			t.Error("Could not get id from result")
		}
		if rId != -1 {
			t.Errorf("Unexpected id %d", rId)
		}
		nRows, err := result.RowsAffected()
		if err != nil {
			t.Error("Could not get number of rows from result")
		}
		if nRows != 1 {
			t.Errorf("Unexpected number of rows %d", nRows)
		}
	})

	t.Run("Get timezone", func(t *testing.T) {
		rows, err := db.Query("select local_timezone()")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		for rows.Next() {
			var name int
			if err := rows.Scan(&name); err != nil {
				t.Error(err)
			}
			if name != 0 {
				t.Error("unexpected value for local timezone")
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		defer rows.Close()
	})

	t.Run("Get timezone other connection", func(t *testing.T) {
		rows, err := db1.Query("select CURRENT_TIMEZONE")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		for rows.Next() {
			var name int
			if err := rows.Scan(&name); err != nil {
				t.Error(err)
			}
			if name != 3600 {
				t.Error("unexpected value for current timezone")
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		defer rows.Close()
	})

	t.Run("Exec drop table", func(t *testing.T) {
		result, err := db.Exec("drop table test1")
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("query did not return a result object")
		}
		rId, err := result.LastInsertId()
		if err != nil {
			t.Error("Could not get id from result")
		}
		if rId != 0 {
			t.Errorf("Unexpected id %d", rId)
		}
		nRows, err := result.RowsAffected()
		if err != nil {
			t.Error("Could not get number of rows from result")
		}
		if nRows != 0 {
			t.Errorf("Unexpected number of rows %d", nRows)
		}
	})
}
