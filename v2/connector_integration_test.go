/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package monetdb

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestConnectorDefaultIntegration(t *testing.T) {
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

	t.Run("Run simple query", func(t *testing.T) {
		rows, err := db.Query("select name from test1")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		defer rows.Close()
	})

	t.Run("Get Columns", func(t *testing.T) {
		rows, err := db.Query("select name from test1")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		defer rows.Close()
		columnlist, err := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columnlist {
			if column != "name" {
				t.Errorf("unexpected column name in Columns: %s", column)
			}
		}
		columntypes, err := rows.ColumnTypes()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columntypes {
			if column.Name() != "name" {
				t.Errorf("unexpected column name in ColumnTypes")
			}
			length, length_ok := column.Length()
			if length_ok != false {
				t.Errorf("unexpected value for length_ok")
			} else {
				if length_ok {
					if length != 0 {
						t.Errorf("unexpected column length in ColumnTypes")
					}
				}
			}
			_, nullable_ok := column.Nullable()
			if nullable_ok != false {
				t.Errorf("not expected that nullable was provided")
			}
			coltype := column.DatabaseTypeName()
			if coltype != "DECIMAL" {
				t.Errorf("unexpected column typename")
			}
			scantype := column.ScanType()
			// Not every type has a name. Then the name is the empty string. In that case, compare the types
			if scantype.Name() != "" {
				if scantype.Name() != "float64" {
					t.Errorf("unexpected scan type: %s instead of %s", "float64", scantype.Name())
				}
			} else {
				if fmt.Sprintf("%v", scantype) != "float64" {
					t.Errorf("unexpected scan type: %s instead of %v", "float64", scantype)
				}
			}
			precision, scale, ok := column.DecimalSize()
			if ok != true {
				t.Errorf("not expected that decimal size was provided")
			} else {
				if ok {
					if precision != 18 {
						t.Errorf("Unexpected value for precision")
					}
					if scale != 3 {
						t.Errorf("unexpected value for scale")
					}
				}
			}
		}
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

func TestConnectorSizeheaderFalseIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	connector, err := NewConnector("monetdb:monetdb@localhost:50000/monetdb", SizeHeaderOption(false))
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

	t.Run("Run simple query", func(t *testing.T) {
		rows, err := db.Query("select name from test1")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		defer rows.Close()
	})

	t.Run("Get Columns", func(t *testing.T) {
		rows, err := db.Query("select name from test1")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		defer rows.Close()
		columnlist, err := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columnlist {
			if column != "name" {
				t.Errorf("unexpected column name in Columns: %s", column)
			}
		}
		columntypes, err := rows.ColumnTypes()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columntypes {
			if column.Name() != "name" {
				t.Errorf("unexpected column name in ColumnTypes")
			}
			length, length_ok := column.Length()
			if length_ok != false {
				t.Errorf("unexpected value for length_ok")
			} else {
				if length_ok {
					if length != 0 {
						t.Errorf("unexpected column length in ColumnTypes")
					}
				}
			}
			_, nullable_ok := column.Nullable()
			if nullable_ok != false {
				t.Errorf("not expected that nullable was provided")
			}
			coltype := column.DatabaseTypeName()
			if coltype != "DECIMAL" {
				t.Errorf("unexpected column typename")
			}
			scantype := column.ScanType()
			// Not every type has a name. Then the name is the empty string. In that case, compare the types
			if scantype.Name() != "" {
				if scantype.Name() != "float64" {
					t.Errorf("unexpected scan type: %s instead of %s", "float64", scantype.Name())
				}
			} else {
				if fmt.Sprintf("%v", scantype) != "float64" {
					t.Errorf("unexpected scan type: %s instead of %v", "float64", scantype)
				}
			}
			// In this case, the value for ok might not be what you expect. It means that precision and scale
			// make sense for the type of the column, not that the values of precision and scale are correct.
			// In this specific test, the database does not provide the correct values for precision and scale,
			// because sizeheader is false. Therefore the values will be 0. This also explains why we need to set
			// sizeheader to true by default.
			precision, scale, ok := column.DecimalSize()
			if ok != true {
				t.Errorf("expected that decimal size was provided")
			} else {
				if ok {
					if precision != 0 {
						t.Errorf("unexpected value for precision")
					}
					if scale != 0 {
						t.Errorf("unexpected value for scale")
					}
				}
			}
		}
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
