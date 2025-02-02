// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

// IDriver opens IContext.
type IDriver interface {
	// OpenCtx opens an IContext with client capability, collation and dbname.
	OpenCtx(capability uint32, collation uint8, dbname string) (IContext, error)
}

// IContext is the interface to execute commant.
type IContext interface {
	// Status returns server status code.
	Status() uint16

	// LastInsertID returns last inserted ID.
	LastInsertID() uint64

	// AffectedRows returns affected rows of last executed command.
	AffectedRows() uint64

	// WarningCount returns warning count of last executed command.
	WarningCount() uint16

	// CurrentDB returns current DB.
	CurrentDB() string

	// Execute executes a SQL statement.
	Execute(sql string) (ResultSet, error)

	// Prepare prepares a statement.
	Prepare(sql string) (statement IStatement, columns, params []*ColumnInfo, err error)

	// GetStatement gets IStatement by statement ID.
	GetStatement(stmtID int) IStatement

	// FieldList returns columns of a table.
	FieldList(tableName string) (columns []*ColumnInfo, err error)

	// Close closes the IContext.
	Close() error
}

// IStatement is the interface to use a prepared statement.
type IStatement interface {
	// ID returns statement ID
	ID() int

	// Execute executes the statement.
	Execute(args ...interface{}) (ResultSet, error)

	// AppendParam appends parameter to the statement.
	AppendParam(paramID int, data []byte) error

	// NumParams returns number of parameters.
	NumParams() int

	// BoundParams returns bound parameters.
	BoundParams() [][]byte

	// Reset removes all bound parameters.
	Reset()

	// Close closes the statement.
	Close() error
}

// ResultSet is the result set of an query.
type ResultSet interface {
	Columns() ([]*ColumnInfo, error)
	Next() ([]interface{}, error)
	Close() error
}
