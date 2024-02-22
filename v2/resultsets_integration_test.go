/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
*/

package monetdb

import (
	"database/sql"
	"testing"
)
 
func TestResultsetSingleIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	if pingErr := db.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}
	defer db.Close()

	t.Run("Exec create table", func(t *testing.T) {
		result, err := db.Exec("create table test1 ( name varchar(16))")
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
		result, err := db.Exec("insert into test1 values ( 'name1' )")
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
		rows, err := db.Query("select * from test1")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		defer rows.Close()

		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Error(err)
			}
		}
	
		if rows.NextResultSet() {
			t.Fatal("unexpected second resultset")
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					t.Error(err)
				}
			}
			if err := rows.Err(); err != nil {
				t.Error(err)
			}
		}

		if err := rows.Err(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Get Columns", func(t *testing.T) {
		rows, err := db.Query("select * from test1")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		defer rows.Close()
		columnlist, err  := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columnlist {
			if column != "name" {
				t.Error("unexpected column name")
			}
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

func TestResultsetMultipleIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	if pingErr := db.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}
	defer db.Close()

	t.Run("Exec create table1", func(t *testing.T) {
		result, err := db.Exec("create table test1 ( name varchar(16))")
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

	t.Run("Exec create table2", func(t *testing.T) {
		result, err := db.Exec("create table test2 ( value int)")
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

	t.Run("Exec insert row 1", func(t *testing.T) {
		result, err := db.Exec("insert into test1 values ( 'name1' )")
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

	t.Run("Exec insert row 2", func(t *testing.T) {
		result, err := db.Exec("insert into test2 values ( 42 )")
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

	t.Run("Run multiple query", func(t *testing.T) {
		rows, err := db.Query("select * from test1; select * from test2;")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		defer rows.Close()

		columnlist, err  := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columnlist {
			// The current implementation is not correct, it does not handle multiple resultsets as expected.
			// The column name should be "name1", from the first table. But we get the schema information of
			// the second table. The current version of the tests uses the wrong version to get the test to
			// pass. We do this to document the incorrect version, before the rewrite.
			if column != "name" {
				t.Error("unexpected column name", column)
			}
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Error(err)
			}
			if name != "name1" {
				t.Error("unexpected value of name is", name)
			}
		}
		if rows.NextResultSet() {
			columnlist, err  := rows.Columns()
			if err != nil {
				t.Error(err)
			}
				for _, column := range columnlist {
				if column != "value" {
					t.Error("unexpected column name", column)
				}
			}

			for rows.Next() {
				var value int
				if err := rows.Scan(&value); err != nil {
					t.Error(err)
				}
			}
			if err := rows.Err(); err != nil {
				t.Error(err)
			}
		}

		if err := rows.Err(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Get Columns 1", func(t *testing.T) {
		rows, err := db.Query("select * from test1")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		defer rows.Close()
		columnlist, err  := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columnlist {
			if column != "name" {
				t.Error("unexpected column name")
			}
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
	})

	t.Run("Get Columns 2", func(t *testing.T) {
		rows, err := db.Query("select * from test2")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		defer rows.Close()
		columnlist, err  := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columnlist {
			if column != "value" {
				t.Error("unexpected column name")
			}
		}
		for rows.Next() {
			var value int
			if err := rows.Scan(&value); err != nil {
			 t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Exec drop table 1", func(t *testing.T) {
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

	t.Run("Exec drop table 2", func(t *testing.T) {
		result, err := db.Exec("drop table test2")
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
