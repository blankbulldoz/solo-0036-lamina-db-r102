package query

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"math"
	"sort"
)

// Execute reads positional tuples and only materializes the external
// name-keyed row shape at the projection boundary.
func Execute(r catalog.Relation, q Request, rows map[string]datum.Tuple, plan Plan, keys []string) (Result, error) {
	result := Result{Rows: []datum.Row{}, Plan: plan}
	if plan.Access == "scan" {
		keys = datum.Keys(rows)
	}
	predicates := compilePredicates(r, q.Where)
	order := compileOrder(r, q.Order)
	primary := position(r, r.Primary)
	selected := make([]datum.Tuple, 0)
	for _, key := range keys {
		row, ok := rows[key]
		if !ok {
			continue
		}
		result.Scanned++
		if matches(row, predicates) {
			selected = append(selected, row)
		}
	}
	result.Matched = len(selected)
	if q.Aggregate != nil {
		row, err := reduce(selected, *q.Aggregate, position(r, q.Aggregate.Column))
		if err != nil {
			return result, err
		}
		result.Rows = append(result.Rows, row)
		return result, nil
	}
	sort.Slice(selected, func(i, j int) bool {
		for _, o := range order {
			n := datum.Order(selected[i][o.Position], selected[j][o.Position])
			if n != 0 {
				if o.Desc {
					return n > 0
				}
				return n < 0
			}
		}
		return datum.Order(selected[i][primary], selected[j][primary]) < 0
	})
	limit := q.Limit
	if limit == 0 {
		limit = 1000
	}
	if len(selected) > limit {
		selected = selected[:limit]
		result.Truncated = true
	}
	columns := q.Columns
	if len(columns) == 0 {
		columns = r.Layout()
	}
	positions := make([]int, len(columns))
	for i, name := range columns {
		positions[i] = position(r, name)
	}
	for _, row := range selected {
		projection := make(datum.Row, len(columns))
		for i, name := range columns {
			projection[name] = row[positions[i]]
		}
		result.Rows = append(result.Rows, projection)
	}
	return result, nil
}

// position is safe only after Bind: every name used here was validated.
func position(r catalog.Relation, name string) int {
	if name == "" || name == "*" {
		return -1
	}
	p, _ := r.Position(name)
	return p
}

type boundPredicate struct {
	Position int
	Op       string
	Value    datum.Value
}

func compilePredicates(r catalog.Relation, where []Predicate) []boundPredicate {
	out := make([]boundPredicate, 0, len(where))
	for _, p := range where {
		out = append(out, boundPredicate{Position: position(r, p.Column), Op: p.Op, Value: p.Value})
	}
	return out
}

func matches(row datum.Tuple, predicates []boundPredicate) bool {
	for _, p := range predicates {
		if p.Position < 0 || p.Position >= len(row) {
			return false
		}
		v := row[p.Position]
		if p.Op == "is_null" {
			if !v.Null() {
				return false
			}
			continue
		}
		if p.Op == "not_null" {
			if v.Null() {
				return false
			}
			continue
		}
		if v.Null() || p.Value.Null() {
			return false
		}
		n, err := v.Compare(p.Value)
		if err != nil {
			return false
		}
		pass := false
		switch p.Op {
		case "=":
			pass = n == 0
		case "!=":
			pass = n != 0
		case "<":
			pass = n < 0
		case ">":
			pass = n > 0
		case "<=":
			pass = n <= 0
		case ">=":
			pass = n >= 0
		}
		if !pass {
			return false
		}
	}
	return true
}

type boundOrder struct {
	Position int
	Desc     bool
}

func compileOrder(r catalog.Relation, order []Ordering) []boundOrder {
	out := make([]boundOrder, 0, len(order))
	for _, o := range order {
		out = append(out, boundOrder{Position: position(r, o.Column), Desc: o.Desc})
	}
	return out
}

func reduce(rows []datum.Tuple, a Aggregate, pos int) (datum.Row, error) {
	var total int64
	var count int64
	for _, row := range rows {
		v := datum.Value{Kind: "null"}
		if pos >= 0 && pos < len(row) {
			v = row[pos]
		}
		if pos >= 0 && v.Null() {
			continue
		}
		count++
		if a.Function == "sum" {
			if v.Int > 0 && total > math.MaxInt64-v.Int || v.Int < 0 && total < math.MinInt64-v.Int {
				return nil, fmt.Errorf("sum overflow")
			}
			total += v.Int
		}
	}
	if a.Function == "count" {
		return datum.Row{"count": datum.Integer(count)}, nil
	}
	if count == 0 {
		return datum.Row{"sum": datum.Null()}, nil
	}
	return datum.Row{"sum": datum.Integer(total)}, nil
}
