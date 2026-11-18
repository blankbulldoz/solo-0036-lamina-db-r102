package journal

import (
	"encoding/json"
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

// CurrentFormat is the durable record generation. Format 1 records carried
// name-keyed rows; format 2 records carry tuples bound to the relation layout.
const CurrentFormat = 2

type Record struct {
	Format      int                 `json:"format"`
	Sequence    uint64              `json:"sequence"`
	Kind        string              `json:"kind"`
	Transaction string              `json:"transaction,omitempty"`
	Relation    *catalog.Relation   `json:"relation,omitempty"`
	Index       *catalog.Index      `json:"index,omitempty"`
	Writes      []versions.Mutation `json:"writes,omitempty"`
	Horizon     uint64              `json:"horizon,omitempty"`

	legacy []versions.LegacyMutation
}

type recordWire struct {
	Format      int               `json:"format"`
	Sequence    uint64            `json:"sequence"`
	Kind        string            `json:"kind"`
	Transaction string            `json:"transaction,omitempty"`
	Relation    *catalog.Relation `json:"relation,omitempty"`
	Index       *catalog.Index    `json:"index,omitempty"`
	Writes      json.RawMessage   `json:"writes,omitempty"`
	Horizon     uint64            `json:"horizon,omitempty"`
}

func (r *Record) UnmarshalJSON(data []byte) error {
	var wire recordWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	r.Format = wire.Format
	r.Sequence = wire.Sequence
	r.Kind = wire.Kind
	r.Transaction = wire.Transaction
	r.Relation = wire.Relation
	r.Index = wire.Index
	r.Horizon = wire.Horizon
	r.Writes = nil
	r.legacy = nil
	if len(wire.Writes) == 0 || string(wire.Writes) == "null" {
		return nil
	}
	switch wire.Format {
	case 1:
		if err := json.Unmarshal(wire.Writes, &r.legacy); err != nil {
			return err
		}
	case 2:
		if err := json.Unmarshal(wire.Writes, &r.Writes); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported record format %d", wire.Format)
	}
	return nil
}

// Legacy reports whether this record still needs catalog-bound conversion.
func (r Record) Legacy() bool { return r.legacy != nil }

// ConvertLegacy binds every old name-keyed row to the relation positions in
// force at this sequence. Any row that cannot be explained rejects conversion.
func (r *Record) ConvertLegacy(c *catalog.Catalog) error {
	if r.legacy == nil {
		return nil
	}
	if r.Format != 1 {
		return fmt.Errorf("legacy writes on format %d record", r.Format)
	}
	writes := make([]versions.Mutation, 0, len(r.legacy))
	for _, old := range r.legacy {
		relation, err := c.Relation(old.Relation)
		if err != nil {
			return fmt.Errorf("sequence %d: %w", r.Sequence, err)
		}
		mutation, err := old.Convert(relation)
		if err != nil {
			return fmt.Errorf("sequence %d: %w", r.Sequence, err)
		}
		writes = append(writes, mutation)
	}
	r.Writes = writes
	r.legacy = nil
	r.Format = CurrentFormat
	return nil
}
