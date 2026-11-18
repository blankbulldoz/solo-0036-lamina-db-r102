package datum

import "encoding/json"

// Tuple is a value list bound to a relation layout. Position i always denotes
// the same catalog column for every version of a table, so column names are
// paid once per relation instead of once per stored row.
type Tuple []Value

func (t Tuple) Clone() Tuple {
	if t == nil {
		return nil
	}
	c := make(Tuple, len(t))
	copy(c, t)
	return c
}
func (t Tuple) MarshalJSON() ([]byte, error) { return json.Marshal([]Value(t)) }
func (t *Tuple) UnmarshalJSON(data []byte) error {
	var values []Value
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	*t = Tuple(values)
	return nil
}
func Null() Value { return Value{Kind: "null"} }
