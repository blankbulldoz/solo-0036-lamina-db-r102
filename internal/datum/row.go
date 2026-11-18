package datum

import "sort"

// Row is the external, name-keyed row shape used by HTTP requests and
// results. It is not the internal storage form.
type Row map[string]Value

func Clone(row Row) Row {
	if row == nil {
		return nil
	}
	c := make(Row, len(row))
	for k, v := range row {
		c[k] = v
	}
	return c
}
func Keys[T any](rows map[string]T) []string {
	keys := make([]string, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
func Order(a, b Value) int {
	if a.Null() && b.Null() {
		return 0
	}
	if a.Null() {
		return -1
	}
	if b.Null() {
		return 1
	}
	n, _ := a.Compare(b)
	return n
}
