/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package mapi

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type query struct {
	mapi   MapiConn
	sqlQuery string
	resultSets []ResultSet
	currentResultSet int
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

func NewQuery(conn MapiConn, q string) Query {
	res := query {
		mapi: conn,
		sqlQuery: q,
		resultSets: make([]ResultSet, 0),
		currentResultSet: -1,
	}
	return &res
}

func (q query) Result() *ResultSet {
	if q.currentResultSet == -1 {
		return nil
	}
	return &q.resultSets[q.currentResultSet]
}

type LineType = int
const (
	PROMT LineType = iota
	INFO
	ERROR
	QTABLE
	QUPDATE
	QSCHEMA
	QTRANS
	QPREPARE
	QBLOCK
	HEADER
	TUPLE
	REDIRECT
	OK
	UNKNOWN
)

func getLineType(line string) LineType {
	var lineType LineType

	// PROMPT is the empty string, so if we used the hasprefix method here, we would
	// always get true. So use the exact match to determine is the line is a prompt
	if (line == mapi_MSG_PROMPT) {
		lineType = PROMT
	} else if (strings.HasPrefix(line, mapi_MSG_INFO)) {
		lineType = INFO
	} else if (strings.HasPrefix(line, mapi_MSG_ERROR)) {
		lineType = ERROR
	} else if (strings.HasPrefix(line, mapi_MSG_QPREPARE)) {
		lineType = QPREPARE
	} else if (strings.HasPrefix(line, mapi_MSG_QTABLE)) {
		lineType = QTABLE
	} else if (strings.HasPrefix(line, mapi_MSG_TUPLE)) {
		lineType = TUPLE
	} else if (strings.HasPrefix(line, mapi_MSG_QBLOCK)) {
		lineType = QBLOCK
	} else if (strings.HasPrefix(line, mapi_MSG_QSCHEMA)) {
		lineType = QSCHEMA
	} else if (strings.HasPrefix(line, mapi_MSG_QUPDATE)) {
		lineType = QUPDATE
	} else if (strings.HasPrefix(line, mapi_MSG_QTRANS)) {
		lineType = QTRANS
	} else if (strings.HasPrefix(line, mapi_MSG_HEADER)) {
		lineType = HEADER
	} else {
		lineType = UNKNOWN
	}
	return lineType
}

func (q *query) newResultSet() {
	r := ResultSet{}
	r.Metadata.ExecId = -1
	q.resultSets = append(q.resultSets, r)
	q.currentResultSet++
}

func (q *query) StoreResult(r string) error {
	var columnNames []string
	var columnTypes []string
	var displaySizes []int
	var internalSizes []int
	var precisions []int
	var scales []int
	var nullOks []int

	var addedResultSets bool

	for _, line := range strings.Split(r, "\n") {
		lineType := getLineType(line)
		if lineType == INFO {
			// TODO log

		} else if lineType == QPREPARE {
			q.newResultSet()

			t := strings.Split(strings.TrimSpace(line[2:]), " ")
			q.Result().Metadata.ExecId, _ = strconv.Atoi(t[0])
			return nil

		} else if lineType == QTABLE {
			q.newResultSet()
			addedResultSets = true

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

		} else if lineType == TUPLE {
			v, err := q.Result().parseTuple(line)
			if err != nil {
				return err
			}
			q.Result().Rows = append(q.Result().Rows, v)

		} else if lineType == QBLOCK {
			q.Result().Rows = make([][]Value, 0)

		} else if lineType == QSCHEMA {
			q.newResultSet()
			addedResultSets = true

			q.Result().Metadata.Offset = 0
			q.Result().Rows = make([][]Value, 0)
			q.Result().Metadata.LastRowId = 0
			q.Result().Schema = nil
			q.Result().Metadata.RowCount = 0

		} else if lineType == QUPDATE {
			if q.currentResultSet == -1 {
				q.newResultSet()
				addedResultSets = true
			}

			t := strings.Split(strings.TrimSpace(line[2:]), " ")
			q.Result().Metadata.RowCount, _ = strconv.Atoi(t[0])
			q.Result().Metadata.LastRowId, _ = strconv.Atoi(t[1])

		} else if lineType == QTRANS {
			q.newResultSet()
			addedResultSets = true

			q.Result().Metadata.Offset = 0
			q.Result().Rows = make([][]Value, 0)
			q.Result().Metadata.LastRowId = 0
			q.Result().Schema = nil
			q.Result().Metadata.RowCount = 0

		} else if lineType == HEADER {
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

		} else if lineType == PROMT {
			// At this point we processed all the data that was returned from
			// the server. In certain cases one or more resultsets have been
			// created, but not in every case. The client wants to start with
			// the first resultset, not the last one. Therefore we need to set
			// the current resultset to the first one.
			if addedResultSets { q.currentResultSet = 0}
			return nil
		} else if lineType == ERROR {
			return fmt.Errorf("mapi: database error: %s", line[1:])
		} else if lineType == UNKNOWN {
			return fmt.Errorf("mapi: protocol error: %s", line)
		}
	}

	return fmt.Errorf("mapi: unknown state: %s", r)
}

func (q *query) FetchNext(offset int, amount int) (string, error) {
	return q.mapi.FetchNext(q.resultSets[q.currentResultSet].Metadata.QueryId, offset, amount)
}

func (q *query) execute(query string) (string, error) {
	if q.mapi == nil {
		return "", fmt.Errorf("mapi: database connection is closed")
	}
	return q.mapi.Execute(query)
}

func (q *query) PrepareQuery() error {
	querystring := fmt.Sprintf("PREPARE %s", q.sqlQuery)
	resultstring, err := q.execute(querystring)

	if err != nil {
		return err
	}
	return q.StoreResult(resultstring)
}

func (q *query) ExecutePreparedQuery(args []Value) (string, error) {
	execStr, err := q.resultSets[q.currentResultSet].CreateExecString(args)
	if err != nil {
		return "", err
	}
	return q.execute(execStr)
}

func (q *query) CreateNamedString(names []string, args []Value) (string, error) {
	var b bytes.Buffer
	// A query with named placeholders ends with a colon, before the named arguments list
	b.WriteString(fmt.Sprintf("%s : ( ", q.sqlQuery))

	for i, v := range args {
		str, err := ConvertToMonet(v)
		if err != nil {
			return "", nil
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf(" %s ", names[i]))
		b.WriteString(str)
	}

		b.WriteString(")")
		return b.String(), nil
	}

func (q *query) ExecuteNamedQuery(names []string, args []Value) (string, error) {
	execStr, err := q.CreateNamedString(names, args)
	if err != nil {
		return "", err
	}
	return q.execute(execStr)
}

func (q query) ExecuteQuery() (string, error) {
	return q.execute(q.sqlQuery)
}

func (q query) HasNextResultSet() bool {
	return (q.currentResultSet != -1) && (len(q.resultSets) > q.currentResultSet + 1 )
}

func (q *query) NextResultSet() error {
	if q.HasNextResultSet() {
		q.currentResultSet++
		return nil
	} else {
		return io.EOF
	}
}
