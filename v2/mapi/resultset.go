/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package mapi

import (
	"bytes"
	"fmt"
	"strings"
)

type TableElement struct {
	ColumnName   string
	ColumnType   string
	DisplaySize  int
	InternalSize int
	Precision    int
	Scale        int
	NullOk       int
}

type Metadata struct {
	ExecId      int
	LastRowId   int
	RowCount    int
	QueryId     int
	Offset      int
	ColumnCount int
}

type Value interface{}

type ResultSet struct {
	Metadata Metadata
	Schema []TableElement
	Rows [][]Value
}

func (s *ResultSet) parseTuple(d string) ([]Value, error) {
	items := strings.Split(d[1:len(d)-1], ",\t")
	if len(items) != len(s.Schema) {
		return nil, fmt.Errorf("mapi: length of row doesn't match header")
	}

	v := make([]Value, len(items))
	for i, value := range items {
		vv, err := s.convert(value, s.Schema[i].ColumnType)
		if err != nil {
			return nil, err
		}
		v[i] = vv
	}
	return v, nil
}

func (s *ResultSet) updateSchema(
	columnNames, columnTypes []string, displaySizes,
	internalSizes, precisions, scales, nullOks []int) {

	d := make([]TableElement, len(columnNames))
	for i, columnName := range columnNames {
		desc := TableElement{
			ColumnName:   columnName,
			ColumnType:   columnTypes[i],
			DisplaySize:  displaySizes[i],
			InternalSize: internalSizes[i],
			Precision:    precisions[i],
			Scale:        scales[i],
			NullOk:       nullOks[i],
		}
		d[i] = desc
	}

	s.Schema = d
}

func (s *ResultSet) convert(value, dataType string) (Value, error) {
	val, err := convertToGo(value, dataType)
	return val, err
}

func (s *ResultSet) CreateExecString(args []Value) (string, error) {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("EXEC %d (", s.Metadata.ExecId))

	for i, v := range args {
		str, err := ConvertToMonet(v)
		if err != nil {
			return "", nil
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(str)
	}

	b.WriteString(")")
	return b.String(), nil
}

func (s *ResultSet) Columns() []string {
	columns := make([]string, len(s.Schema))
	for i, d := range s.Schema {
		columns[i] = d.ColumnName
	}
	return columns
}
