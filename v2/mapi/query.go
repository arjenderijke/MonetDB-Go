/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package mapi

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

type query struct {
	mapi   *MapiConn
	sqlQuery string
	resultSets []ResultSet
}

/* The Query interface type handles the execution of a sql query, that can contain multiple
 * sql statements, separated by a semi-colon. Therefore the query can produce multiple
 * resultsets. This type is part of the mapi library. Therefore we want to try and keep
 * anything out that is not strictly related to the Mapi protocol. This is why we do not
 * implement handling of the Context here, but in the monetdb driver itself. The consequence
 * is that we cannot automatically store the result of a query, but that this function has
 * to be explicitly called by the monetdb driver functions that execute sql queries.
*/

type Query interface {
	PrepareQuery() error
	ExecuteQuery() (string, error)
	ExecutePreparedQuery(args []Value) (string, error)
	ExecuteNamedQuery(names []string, args []Value) (string, error)
	Result() *ResultSet
	StoreResult(r string) error
	FetchNext(offset int, amount int) (string, error)
	HasNextResultSet() bool
	NextResultSet() error
}

func NewQuery(conn *MapiConn, q string) Query {
	res := query {
		mapi: conn,
		sqlQuery: q,
		resultSets: make([]ResultSet, 0),
	}
	r := new(ResultSet)
	r.Metadata.ExecId = -1
	res.resultSets = append(res.resultSets, *r)
	return res
}

func (q query) Result() *ResultSet {
	return &q.resultSets[0]
}

func (q query) StoreResult(r string) error {
	var columnNames []string
	var columnTypes []string
	var displaySizes []int
	var internalSizes []int
	var precisions []int
	var scales []int
	var nullOks []int

	for _, line := range strings.Split(r, "\n") {
		if strings.HasPrefix(line, mapi_MSG_INFO) {
			// TODO log

		} else if strings.HasPrefix(line, mapi_MSG_QPREPARE) {
			t := strings.Split(strings.TrimSpace(line[2:]), " ")
			q.Result().Metadata.ExecId, _ = strconv.Atoi(t[0])
			return nil

		} else if strings.HasPrefix(line, mapi_MSG_QTABLE) {
			t := strings.Split(strings.TrimSpace(line[2:]), " ")
			q.Result().Metadata.QueryId, _ = strconv.Atoi(t[0])
			q.Result().Metadata.RowCount, _ = strconv.Atoi(t[1])
			q.Result().Metadata.ColumnCount, _ = strconv.Atoi(t[2])

			columnNames = make([]string, q.Result().Metadata.ColumnCount)
			columnTypes = make([]string, q.Result().Metadata.ColumnCount)
			displaySizes = make([]int, q.Result().Metadata.ColumnCount)
			internalSizes = make([]int, q.Result().Metadata.ColumnCount)
			precisions = make([]int, q.Result().Metadata.ColumnCount)
			scales = make([]int, q.Result().Metadata.ColumnCount)
			nullOks = make([]int, q.Result().Metadata.ColumnCount)

		} else if strings.HasPrefix(line, mapi_MSG_TUPLE) {
			v, err := q.Result().parseTuple(line)
			if err != nil {
				return err
			}
			q.Result().Rows = append(q.Result().Rows, v)

		} else if strings.HasPrefix(line, mapi_MSG_QBLOCK) {
			q.Result().Rows = make([][]Value, 0)

		} else if strings.HasPrefix(line, mapi_MSG_QSCHEMA) {
			q.Result().Metadata.Offset = 0
			q.Result().Rows = make([][]Value, 0)
			q.Result().Metadata.LastRowId = 0
			q.Result().Schema = nil
			q.Result().Metadata.RowCount = 0

		} else if strings.HasPrefix(line, mapi_MSG_QUPDATE) {
			t := strings.Split(strings.TrimSpace(line[2:]), " ")
			q.Result().Metadata.RowCount, _ = strconv.Atoi(t[0])
			q.Result().Metadata.LastRowId, _ = strconv.Atoi(t[1])

		} else if strings.HasPrefix(line, mapi_MSG_QTRANS) {
			q.Result().Metadata.Offset = 0
			q.Result().Rows = make([][]Value, 0)
			q.Result().Metadata.LastRowId = 0
			q.Result().Schema = nil
			q.Result().Metadata.RowCount = 0

		} else if strings.HasPrefix(line, mapi_MSG_HEADER) {
			t := strings.Split(line[1:], "#")
			data := strings.TrimSpace(t[0])
			identity := strings.TrimSpace(t[1])

			values := make([]string, 0)
			for _, value := range strings.Split(data, ",") {
				values = append(values, strings.TrimSpace(value))
			}

			if identity == "name" {
				columnNames = values

			} else if identity == "type" {
				columnTypes = values

			} else if identity == "typesizes" {
				sizes := make([][]int, len(values))
				for i, value := range values {
					s := make([]int, 0)
					for _, v := range strings.Split(value, " ") {
						val, _ := strconv.Atoi(v)
						s = append(s, val)
					}
					internalSizes[i] = s[0]
					sizes[i] = s
				}
				for j, t := range columnTypes {
					if t == "decimal" {
						precisions[j] = sizes[j][0]
						scales[j] = sizes[j][1]
					}
				}
			} else if identity == "length" {
				for i, value := range values {
					s := make([]int, 0)
					for _, v := range strings.Split(value, " ") {
						val, _ := strconv.Atoi(v)
						s = append(s, val)
					}
					displaySizes[i] = s[0]
				}
			}

			q.Result().updateSchema(columnNames, columnTypes, displaySizes,
				internalSizes, precisions, scales, nullOks)
			q.Result().Metadata.Offset = 0
			q.Result().Metadata.LastRowId = 0

		} else if strings.HasPrefix(line, mapi_MSG_PROMPT) {
			return nil

		} else if strings.HasPrefix(line, mapi_MSG_ERROR) {
			return fmt.Errorf("mapi: database error: %s", line[1:])
		}
	}

	return fmt.Errorf("mapi: unknown state: %s", r)
}

func (q query) FetchNext(offset int, amount int) (string, error) {
	return q.mapi.fetchNext(q.resultSets[0].Metadata.QueryId, offset, amount)
}

func (q query) execute(query string) (string, error) {
	if q.mapi == nil {
		return "", fmt.Errorf("mapi: database connection is closed")
	}
	return q.mapi.Execute(query)
}

func (q query) PrepareQuery() error {
	querystring := fmt.Sprintf("PREPARE %s", q.sqlQuery)
	resultstring, err := q.execute(querystring)

	if err != nil {
		return err
	}
	return q.StoreResult(resultstring)
}

func (q query) ExecutePreparedQuery(args []Value) (string, error) {
	execStr, err := q.resultSets[0].CreateExecString(args)
	if err != nil {
		return "", err
	} 
	return q.execute(execStr)
}

func (q query) ExecuteNamedQuery(names []string, args []Value) (string, error) {
	execStr, err := q.resultSets[0].CreateNamedString(q.sqlQuery, names, args)
	if err != nil {
		return "", err
	}
	return q.execute(execStr)
}

func (q query) ExecuteQuery() (string, error) {
	return q.execute(q.sqlQuery)
}

func (q query) HasNextResultSet() bool {
	return false
}

func (q query) NextResultSet() error {
	return io.EOF
}
