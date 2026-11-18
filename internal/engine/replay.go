package engine

import (
	"fmt"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
)

func (d *DB) apply(r journal.Record) error {
	if r.Sequence != d.sequence+1 || (r.Format != 1 && r.Format != journal.CurrentFormat) {
		return fmt.Errorf("invalid record sequence")
	}
	switch r.Kind {
	case "relation":
		if r.Relation == nil || r.Relation.Created != r.Sequence {
			return fmt.Errorf("invalid relation record")
		}
		if err := r.Relation.Validate(); err != nil {
			return err
		}
		if _, ok := d.catalog.Relations[r.Relation.Name]; ok {
			return fmt.Errorf("duplicate relation")
		}
		d.catalog.Relations[r.Relation.Name] = *r.Relation
		d.store.Ensure(r.Relation.Name)
	case "index":
		if r.Index == nil || r.Index.Created != r.Sequence {
			return fmt.Errorf("invalid index record")
		}
		if err := d.catalog.CheckIndex(*r.Index); err != nil {
			return err
		}
		d.catalog.Indexes[r.Index.Name] = *r.Index
	case "commit":
		writes, err := d.positionalWrites(r.Writes)
		if err != nil {
			return err
		}
		d.store.Apply(r.Sequence, writes)
	case "vacuum":
		if r.Horizon < d.horizon || r.Horizon > d.sequence {
			return fmt.Errorf("invalid vacuum horizon")
		}
		d.store.Vacuum(r.Horizon)
		d.horizon = r.Horizon
	default:
		return fmt.Errorf("unknown record kind %s", r.Kind)
	}
	d.sequence = r.Sequence
	return nil
}

// positionalWrites validates that every mutation row matches the relation
// layout that owns its column positions, then binds the durable key.
func (d *DB) positionalWrites(writes []versions.Mutation) ([]versions.Mutation, error) {
	out := make([]versions.Mutation, 0, len(writes))
	seen := map[string]bool{}
	for _, m := range writes {
		relation, err := d.catalog.Relation(m.Relation)
		if err != nil {
			return nil, err
		}
		token := m.Relation + "\x00" + m.Key
		if seen[token] {
			return nil, fmt.Errorf("duplicate mutation")
		}
		seen[token] = true
		if m.Deleted {
			if len(m.Row) != 0 {
				return nil, fmt.Errorf("tombstone carries a tuple")
			}
			out = append(out, m)
			continue
		}
		if err := relation.ValidateTuple(m.Row); err != nil {
			return nil, err
		}
		key, err := relation.KeyTuple(m.Row)
		if err != nil || key != m.Key {
			return nil, fmt.Errorf("stored primary mismatch")
		}
		out = append(out, m)
	}
	return out, nil
}

func overlay(rows map[string]datum.Tuple, writes map[string]map[string]staged, table string) {
	for key, m := range writes[table] {
		if m.Mutation.Deleted {
			delete(rows, key)
		} else {
			rows[key] = m.Mutation.Row.Clone()
		}
	}
}
