package journal

import (
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

// PublicFormat is the change-feed record schema. It remains 1 so that existing
// consumers keep reading name-keyed rows even though durable frames moved to
// positional tuples.
const PublicFormat = 1

type PublicRecord struct {
	Format      int                      `json:"format"`
	Sequence    uint64                   `json:"sequence"`
	Kind        string                   `json:"kind"`
	Transaction string                   `json:"transaction,omitempty"`
	Relation    *catalog.Relation        `json:"relation,omitempty"`
	Index       *catalog.Index           `json:"index,omitempty"`
	Writes      []versions.NamedMutation `json:"writes,omitempty"`
	Horizon     uint64                   `json:"horizon,omitempty"`
}

// Public renders a durable record in the unchanged public shape. Column names
// are restored from the owning catalog, so no internal layout escapes.
func (r Record) Public(c *catalog.Catalog) PublicRecord {
	out := PublicRecord{Format: PublicFormat, Sequence: r.Sequence, Kind: r.Kind, Transaction: r.Transaction, Relation: r.Relation, Index: r.Index, Horizon: r.Horizon}
	for _, m := range r.Writes {
		named := versions.NamedMutation{Relation: m.Relation, Key: m.Key, Deleted: m.Deleted}
		if !m.Deleted {
			if relation, err := c.Relation(m.Relation); err == nil {
				named.Row = relation.Decode(m.Row)
			}
		}
		out.Writes = append(out.Writes, named)
	}
	return out
}
