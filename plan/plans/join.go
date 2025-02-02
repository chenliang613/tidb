// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ plan.Plan = (*JoinPlan)(nil)
)

// Ref: http://www.w3schools.com/sql/sql_join.asp
const (
	// CrossJoin are used to combine rows from two or more tables
	CrossJoin = "CROSS"
	// LeftJoin returns all rows from the left table (table1), with the matching rows in the right table (table2). The result is NULL in the right side when there is no match.
	LeftJoin = "LEFT"
	// RightJoin returns all rows from the right table (table2), with the matching rows in the left table (table1). The result is NULL in the left side when there is no match.
	RightJoin = "RIGHT"
	// FullJoin returns all rows from the left table (table1) and from the right table (table2).
	FullJoin = "FULL"
)

// JoinPlan handles JOIN query.
// The whole join plan is a tree
// e.g, from (t1 left join t2 on t1.c1 = t2.c2), (t3 right join t4 on t3.c1 = t4.c1)
// the executing order maylook:
//           Table Result
//                |
//          -------------
//         |             |
//      t1 x t2       t3 x t4
//
// TODO: add Parent field, optimize join plan
type JoinPlan struct {
	Left  plan.Plan
	Right plan.Plan

	Type string

	Fields      []*field.ResultField
	On          expression.Expression
	curRow      *plan.Row
	matchedRows []*plan.Row
	cursor      int
	evalArgs    map[interface{}]interface{}
}

// Explain implements plan.Plan Explain interface.
func (r *JoinPlan) Explain(w format.Formatter) {
	// TODO: show more useful join plan
	if r.Right == nil {
		// if right is nil, we don't do a join, just simple select table
		r.Left.Explain(w)
		return
	}

	w.Format("┌Compute %s Cartesian product of\n", r.Type)

	r.explainNode(w, r.Left)
	r.explainNode(w, r.Right)

	w.Format("└Output field names %v\n", field.RFQNames(r.Fields))
}

func (r *JoinPlan) explainNode(w format.Formatter, node plan.Plan) {
	sel := !isTableOrIndex(node)
	if sel {
		w.Format("┌Iterate all rows of virtual table\n")
	}
	node.Explain(w)
	if sel {
		w.Format("└Output field names %v\n", field.RFQNames(node.GetFields()))
	}

}

func (r *JoinPlan) filterNode(ctx context.Context, expr expression.Expression, node plan.Plan) (plan.Plan, bool, error) {
	if node == nil {
		return r, false, nil
	}

	e2, err := expr.Clone()
	if err != nil {
		return nil, false, err
	}

	return node.Filter(ctx, e2)
}

// Filter implements plan.Plan Filter interface, it returns one of the two
// plans' Filter result, maybe we could do some optimizations here.
func (r *JoinPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	// TODO: do more optimization for join plan
	// now we only use where expression for Filter, but for join
	// we must use On expression too.

	p, filtered, err := r.filterNode(ctx, expr, r.Left)
	if err != nil {
		return nil, false, err
	}
	if filtered {
		r.Left = p
		return r, true, nil
	}

	p, filtered, err = r.filterNode(ctx, expr, r.Right)
	if err != nil {
		return nil, false, err
	}
	if filtered {
		r.Right = p
		return r, true, nil
	}
	return r, false, nil
}

// GetFields implements plan.Plan GetFields interface.
func (r *JoinPlan) GetFields() []*field.ResultField {
	return r.Fields
}

// Next implements plan.Plan Next interface.
func (r *JoinPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.Right == nil {
		return r.Left.Next(ctx)
	}
	if r.evalArgs == nil {
		r.evalArgs = map[interface{}]interface{}{}
	}
	switch r.Type {
	case LeftJoin:
		return r.nextLeftJoin(ctx)
	case RightJoin:
		return r.nextRightJoin(ctx)
	default:
		return r.nextCrossJoin(ctx)
	}
}

func (r *JoinPlan) nextLeftJoin(ctx context.Context) (row *plan.Row, err error) {
	for {
		if r.cursor < len(r.matchedRows) {
			row = r.matchedRows[r.cursor]
			r.cursor++
			return
		}
		r.cursor = 0
		var leftRow *plan.Row
		leftRow, err = r.Left.Next(ctx)
		if leftRow == nil || err != nil {
			return nil, errors.Trace(err)
		}
		err = r.findMatchedRows(ctx, leftRow, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
}

func (r *JoinPlan) nextRightJoin(ctx context.Context) (row *plan.Row, err error) {
	for {
		if r.cursor < len(r.matchedRows) {
			row = r.matchedRows[r.cursor]
			r.cursor++
			return
		}
		var rightRow *plan.Row
		rightRow, err = r.Right.Next(ctx)
		if rightRow == nil || err != nil {
			return nil, errors.Trace(err)
		}
		err = r.findMatchedRows(ctx, rightRow, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
}

func (r *JoinPlan) findMatchedRows(ctx context.Context, row *plan.Row, right bool) (err error) {
	var p plan.Plan
	if right {
		p = r.Left
	} else {
		p = r.Right
	}
	r.cursor = 0
	r.matchedRows = nil
	p.Close()
	for {
		var cmpRow *plan.Row
		cmpRow, err = p.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if cmpRow == nil {
			break
		}
		var joined []interface{}
		if right {
			joined = append(cmpRow.Data, row.Data...)
		} else {
			joined = append(row.Data, cmpRow.Data...)
		}
		r.evalArgs[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
			return GetIdentValue(name, r.Fields, joined, field.DefaultFieldFlag)
		}
		var b bool
		b, err = expressions.EvalBoolExpr(ctx, r.On, r.evalArgs)
		if err != nil {
			return errors.Trace(err)
		}
		if b {
			cmpRow.Data = joined
			cmpRow.RowKeys = append(row.RowKeys, cmpRow.RowKeys...)
			r.matchedRows = append(r.matchedRows, cmpRow)
		}
	}
	if len(r.matchedRows) == 0 {
		if right {
			leftLen := len(r.Fields) - len(r.Right.GetFields())
			row.Data = append(make([]interface{}, leftLen), row.Data...)
		} else {
			rightLen := len(r.Fields) - len(r.Left.GetFields())
			row.Data = append(row.Data, make([]interface{}, rightLen)...)
		}
		r.matchedRows = append(r.matchedRows, row)
	}
	return nil
}

func (r *JoinPlan) nextCrossJoin(ctx context.Context) (row *plan.Row, err error) {
	for {
		if r.curRow == nil {
			r.curRow, err = r.Left.Next(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if r.curRow == nil {
				return nil, nil
			}
		}
		var rightRow *plan.Row
		rightRow, err = r.Right.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rightRow == nil {
			r.curRow = nil
			r.Right.Close()
			continue
		}
		joinedRow := append(r.curRow.Data, rightRow.Data...)
		if r.On != nil {
			r.evalArgs[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
				return GetIdentValue(name, r.Fields, joinedRow, field.DefaultFieldFlag)
			}

			b, err := expressions.EvalBoolExpr(ctx, r.On, r.evalArgs)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !b {
				// If On condition not satisified, drop this row
				continue
			}
		}
		row = &plan.Row{
			Data:    joinedRow,
			RowKeys: append(r.curRow.RowKeys, rightRow.RowKeys...),
		}
		return
	}
}

// Close implements plan.Plan Close interface.
func (r *JoinPlan) Close() error {
	r.curRow = nil
	r.matchedRows = nil
	r.cursor = 0
	if r.Right != nil {
		r.Right.Close()
	}
	return r.Left.Close()
}
