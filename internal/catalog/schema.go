package catalog

import (
	"fmt"
	"lamina/internal/datum"
	"regexp"
)

var identifier = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func ValidName(s string) bool { return identifier.MatchString(s) }

type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}
type Relation struct {
	Name    string   `json:"name"`
	Primary string   `json:"primary"`
	Columns []Column `json:"columns"`
	Created uint64   `json:"created"`
}

func (r Relation) Validate() error {
	if !ValidName(r.Name) || !ValidName(r.Primary) {
		return fmt.Errorf("invalid relation or primary name")
	}
	if len(r.Columns) == 0 || len(r.Columns) > 64 {
		return fmt.Errorf("relation needs 1..64 columns")
	}
	seen := map[string]bool{}
	found := false
	for _, c := range r.Columns {
		if !ValidName(c.Name) || seen[c.Name] {
			return fmt.Errorf("invalid or duplicate column %q", c.Name)
		}
		seen[c.Name] = true
		if c.Type != "int" && c.Type != "text" && c.Type != "bool" {
			return fmt.Errorf("unsupported type %q", c.Type)
		}
		if c.Name == r.Primary {
			found = true
			if c.Nullable || c.Type == "bool" {
				return fmt.Errorf("primary must be non-null int or text")
			}
		}
	}
	if !found {
		return fmt.Errorf("primary column does not exist")
	}
	return nil
}
func (r Relation) Column(name string) (Column, bool) {
	for _, c := range r.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}
func (r Relation) Position(name string) (int, bool) {
	for i, c := range r.Columns {
		if c.Name == name {
			return i, true
		}
	}
	return 0, false
}
func (r Relation) Layout() []string {
	out := make([]string, len(r.Columns))
	for i, c := range r.Columns {
		out[i] = c.Name
	}
	return out
}

// Encode validates an external name-keyed row once at the write boundary and
// returns the positional tuple that is stored for every version. The column
// order is the durable catalog order, never the request's key order.
func (r Relation) Encode(row datum.Row) (datum.Tuple, error) {
	normalized, err := r.Normalize(row)
	if err != nil {
		return nil, err
	}
	return r.Tuple(normalized), nil
}

// Tuple reorders a normalized row into catalog column positions.
func (r Relation) Tuple(row datum.Row) datum.Tuple {
	out := make(datum.Tuple, len(r.Columns))
	for i, c := range r.Columns {
		out[i] = row[c.Name]
	}
	return out
}

// Decode reproduces the public row shape from a stored tuple.
func (r Relation) Decode(t datum.Tuple) datum.Row {
	out := make(datum.Row, len(r.Columns))
	for i, c := range r.Columns {
		v := datum.Value{Kind: "null"}
		if i < len(t) {
			v = t[i]
		}
		out[c.Name] = v
	}
	return out
}

// ValidateTuple checks that every slot has the catalog's column meaning.
func (r Relation) ValidateTuple(t datum.Tuple) error {
	if len(t) != len(r.Columns) {
		return fmt.Errorf("tuple width %d does not match relation %s layout %d", len(t), r.Name, len(r.Columns))
	}
	for i, c := range r.Columns {
		v := t[i]
		if v.Null() {
			if !c.Nullable {
				return fmt.Errorf("column %s cannot be null", c.Name)
			}
			continue
		}
		if v.Kind != c.Type {
			return fmt.Errorf("column %s requires %s", c.Name, c.Type)
		}
		if v.Kind == "text" && len(v.Text) > 65536 {
			return fmt.Errorf("text exceeds 64KiB")
		}
	}
	return nil
}

// KeyTuple derives the canonical primary key from the primary column position.
func (r Relation) KeyTuple(t datum.Tuple) (string, error) {
	pos, ok := r.Position(r.Primary)
	if !ok || pos >= len(t) {
		return "", fmt.Errorf("primary column does not exist")
	}
	if t[pos].Null() {
		return "", fmt.Errorf("primary cannot be null")
	}
	return t[pos].Key(), nil
}
func (r Relation) Normalize(row datum.Row) (datum.Row, error) {
	for k := range row {
		if _, ok := r.Column(k); !ok {
			return nil, fmt.Errorf("unknown column %q", k)
		}
	}
	out := make(datum.Row, len(r.Columns))
	for _, c := range r.Columns {
		v, exists := row[c.Name]
		if !exists || v.Null() {
			if !c.Nullable {
				return nil, fmt.Errorf("column %s cannot be null", c.Name)
			}
			v = datum.Value{Kind: "null"}
		}
		if !v.Null() && v.Kind != c.Type {
			return nil, fmt.Errorf("column %s requires %s", c.Name, c.Type)
		}
		if v.Kind == "text" && len(v.Text) > 65536 {
			return nil, fmt.Errorf("text exceeds 64KiB")
		}
		out[c.Name] = v
	}
	return out, nil
}
func (r Relation) Key(row datum.Row) string { return row[r.Primary].Key() }
