package query

import (
	"fmt"
	"lamina/internal/catalog"
)

func Bind(r catalog.Relation, q Request) error {
	if q.Limit < 0 || q.Limit > 10000 {
		return fmt.Errorf("limit must be 0..10000")
	}
	if len(q.Where) > 32 || len(q.Order) > 8 {
		return fmt.Errorf("query expression limit exceeded")
	}
	if q.Aggregate != nil && len(q.Columns) > 0 {
		return fmt.Errorf("projection and aggregate cannot be combined")
	}
	for _, name := range q.Columns {
		if _, ok := r.Column(name); !ok {
			return fmt.Errorf("unknown projection %s", name)
		}
	}
	for _, o := range q.Order {
		if _, ok := r.Column(o.Column); !ok {
			return fmt.Errorf("unknown ordering %s", o.Column)
		}
	}
	for _, p := range q.Where {
		c, ok := r.Column(p.Column)
		if !ok {
			return fmt.Errorf("unknown predicate %s", p.Column)
		}
		switch p.Op {
		case "=", "!=", "<", ">", "<=", ">=", "is_null", "not_null":
		default:
			return fmt.Errorf("unsupported comparison %s", p.Op)
		}
		if p.Op != "is_null" && p.Op != "not_null" && !p.Value.Null() && p.Value.Kind != c.Type {
			return fmt.Errorf("predicate type mismatch for %s", p.Column)
		}
	}
	if a := q.Aggregate; a != nil {
		if a.Function != "count" && a.Function != "sum" {
			return fmt.Errorf("unsupported aggregate")
		}
		if a.Column == "*" || a.Column == "" {
			if a.Function != "count" {
				return fmt.Errorf("sum needs integer column")
			}
		} else {
			c, ok := r.Column(a.Column)
			if !ok {
				return fmt.Errorf("unknown aggregate column")
			}
			if a.Function == "sum" && c.Type != "int" {
				return fmt.Errorf("sum requires integer column")
			}
		}
	}
	return nil
}
