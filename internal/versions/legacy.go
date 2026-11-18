package versions

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/datum"
)

// NamedMutation is the external name-keyed write shape. It reads format-1
// materials and renders change records for public consumers, whose protocol
// did not change.
type NamedMutation struct {
	Relation string    `json:"relation"`
	Key      string    `json:"key"`
	Row      datum.Row `json:"row,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
}

// LegacyMutation is the format-1 durable write shape.
type LegacyMutation = NamedMutation

// The remaining types describe format-1 storage. They are read-only migration
// inputs: version chains built from them are converted to positional tuples
// before any query, edit or publish can observe them.
type LegacyVersion struct {
	Sequence uint64    `json:"sequence"`
	Row      datum.Row `json:"row,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
}

// ConvertTable validates one relation's old rows against the catalog that owns
// its column positions and returns the positional chains. Callers stream tables
// one at a time and drop each legacy map, so a whole second database is never
// held at once. A row whose names, nullability, type or primary value disagree
// rejects the conversion instead of being approximated.
func ConvertTable(name string, rows map[string][]LegacyVersion, c *catalog.Catalog) (map[string][]Version, error) {
	relation, err := c.Relation(name)
	if err != nil {
		return nil, fmt.Errorf("legacy table %s: %w", name, err)
	}
	out := make(map[string][]Version, len(rows))
	for key, chain := range rows {
		if len(chain) == 0 {
			return nil, fmt.Errorf("legacy table %s has empty version chain", name)
		}
		converted := make([]Version, 0, len(chain))
		var last uint64
		for _, legacy := range chain {
			if legacy.Sequence <= last {
				return nil, fmt.Errorf("legacy table %s has unordered versions", name)
			}
			last = legacy.Sequence
			version := Version{Sequence: legacy.Sequence, Deleted: legacy.Deleted}
			if !legacy.Deleted {
				row, err := relation.Normalize(legacy.Row)
				if err != nil {
					return nil, fmt.Errorf("legacy %s at %d: %w", name, legacy.Sequence, err)
				}
				if relation.Key(row) != key {
					return nil, fmt.Errorf("legacy %s at %d: primary does not match key", name, legacy.Sequence)
				}
				version.Row = relation.Tuple(row)
			}
			converted = append(converted, version)
		}
		out[key] = converted
	}
	return out, nil
}

func (m NamedMutation) Convert(relation catalog.Relation) (Mutation, error) {
	out := Mutation{Relation: m.Relation, Key: m.Key, Deleted: m.Deleted}
	if m.Deleted {
		return out, nil
	}
	row, err := relation.Normalize(m.Row)
	if err != nil {
		return out, err
	}
	if relation.Key(row) != m.Key {
		return out, fmt.Errorf("legacy primary does not match stored key")
	}
	out.Row = relation.Tuple(row)
	return out, nil
}
