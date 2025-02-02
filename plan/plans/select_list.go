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

package plans

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
)

// SelectList contains real select list defined in select statement which will be output to client
// and hidden list which will just be used internally for order by, having clause, etc, why?
// After we do where phase in select, the left flow are group by -> having -> select fields -> order by -> limit -> final.
// for MySQL, order by may use values not in select fields, e.g select c1 from t order by c2, to support this,
// we should add extra fields in select list, and we will use HiddenOffset to control these fields not to be output.
type SelectList struct {
	Fields       []*field.Field
	ResultFields []*field.ResultField
	AggFields    map[int]struct{}

	// HiddenFieldOffset distinguishes select field list and hidden fields for internal use.
	// We will use this to get select filed list and calculate distinct key.
	HiddenFieldOffset int

	// FromFields is the fields from table.
	FromFields []*field.ResultField
}

func (s *SelectList) updateFields(table string, resultFields []*field.ResultField) {
	// TODO: check database name later.
	for _, v := range resultFields {
		if table == "" || table == v.TableName {
			name := field.JoinQualifiedName("", v.TableName, v.Name)

			f := &field.Field{
				Expr: &expressions.Ident{
					CIStr: model.NewCIStr(name),
				},
				Name: name,
			}

			s.AddField(f, v.Clone())
		}
	}
}

// AddField adds Field and ResultField objects to SelectList, and if result is nil,
// constructs a new ResultField.
func (s *SelectList) AddField(f *field.Field, result *field.ResultField) {
	if result == nil {
		result = &field.ResultField{Name: f.Name}
	}

	s.Fields = append(s.Fields, f)
	s.ResultFields = append(s.ResultFields, result)
}

func (s *SelectList) resolveAggFields() {
	for i, v := range s.Fields {
		if expressions.ContainAggregateFunc(v.Expr) {
			s.AggFields[i] = struct{}{}
		}
	}
}

// GetFields returns ResultField.
func (s *SelectList) GetFields() []*field.ResultField {
	return s.ResultFields
}

// UpdateAggFields adds aggregate function resultfield to select result field list.
func (s *SelectList) UpdateAggFields(expr expression.Expression, tableFields []*field.ResultField) (expression.Expression, error) {
	// For aggregate function, the name can be in table or select list.
	names := expressions.MentionedColumns(expr)

	for _, name := range names {
		if field.ContainFieldName(name, tableFields, field.DefaultFieldFlag) {
			continue
		}

		if field.ContainFieldName(name, s.ResultFields, field.DefaultFieldFlag) {
			continue
		}

		return nil, errors.Errorf("Unknown column '%s'", name)
	}

	// We must add aggregate function to hidden select list
	// and use a position expression to fetch its value later.
	exprName := expr.String()
	if !field.ContainFieldName(exprName, s.ResultFields, field.CheckFieldFlag) {
		f := &field.Field{Expr: expr, Name: exprName}
		resultField := &field.ResultField{Name: exprName}
		s.AddField(f, resultField)

		return &expressions.Position{N: len(s.Fields), Name: exprName}, nil
	}

	return nil, nil
}

// CloneHiddenField checks and clones field and result field from table fields,
// and adds them to hidden field of select list.
func (s *SelectList) CloneHiddenField(name string, tableFields []*field.ResultField) bool {
	// Check and add hidden field.
	if field.ContainFieldName(name, tableFields, field.CheckFieldFlag) {
		resultField, _ := field.CloneFieldByName(name, tableFields, field.CheckFieldFlag)
		f := &field.Field{
			Expr: &expressions.Ident{
				CIStr: resultField.ColumnInfo.Name,
			},
			Name: resultField.Name,
		}
		s.AddField(f, resultField)
		return true
	}

	return false
}

// ResolveSelectList gets fields and result fields from selectFields and srcFields,
// including field validity check and wildcard field processing.
func ResolveSelectList(selectFields []*field.Field, srcFields []*field.ResultField) (*SelectList, error) {
	selectList := &SelectList{
		Fields:       make([]*field.Field, 0, len(selectFields)),
		ResultFields: make([]*field.ResultField, 0, len(selectFields)),
		AggFields:    make(map[int]struct{}),
		FromFields:   srcFields,
	}

	wildcardNum := 0
	for _, v := range selectFields {
		// Check metioned field.
		names := expressions.MentionedColumns(v.Expr)
		if len(names) == 0 {
			selectList.AddField(v, nil)
			continue
		}

		// Check wildcard field.
		name := names[0]
		table, ok, err := field.CheckWildcardField(name)
		if err != nil {
			return nil, err
		}
		if ok {
			// Check unqualified wildcard field number,
			// like `select *, * from t`.
			if table == "" {
				wildcardNum++
				if wildcardNum > 1 {
					return nil, errors.Errorf("wildcard field exist more than once")
				}
			}

			selectList.updateFields(table, srcFields)
			continue
		}

		var result *field.ResultField
		if err = field.CheckAllFieldNames(names, srcFields, field.DefaultFieldFlag); err != nil {
			return nil, errors.Trace(err)
		}

		if _, ok := v.Expr.(*expressions.Ident); ok {
			// Field is ident.
			if result, err = field.CloneFieldByName(name, srcFields, field.DefaultFieldFlag); err != nil {
				return nil, errors.Trace(err)
			}

			// Maybe alias name or only column name.
			if !expressions.IsQualified(v.Name) {
				result.Name = v.Name
			}
		} else {
			// The field is not an ident, maybe binary expression,
			// like `select c1 + c2`, or `select c1 + 10`, etc.
			result = &field.ResultField{Name: v.Name}
		}

		selectList.AddField(v, result)
	}

	selectList.HiddenFieldOffset = len(selectList.Fields)
	selectList.resolveAggFields()

	if selectList.HiddenFieldOffset == 0 {
		return nil, errors.Errorf("invalid empty select fields")
	}

	return selectList, nil
}
