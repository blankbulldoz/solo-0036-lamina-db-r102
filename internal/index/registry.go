package index

import (
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

type Registry struct{ Trees map[string]*Tree }

func Build(c *catalog.Catalog, s *versions.Store) *Registry {
	r := &Registry{Trees: map[string]*Tree{}}
	for name, definition := range c.Indexes {
		tree := New()
		relation, err := c.Relation(definition.Relation)
		if err != nil {
			continue
		}
		pos, ok := relation.Position(definition.Column)
		if !ok {
			continue
		}
		for key, chain := range s.Tables[definition.Relation] {
			for _, v := range chain {
				if !v.Deleted && pos < len(v.Row) {
					tree.Add(v.Row[pos], key)
				}
			}
		}
		r.Trees[name] = tree
	}
	return r
}
func (r *Registry) Apply(c *catalog.Catalog, writes []versions.Mutation) {
	for _, m := range writes {
		if m.Deleted {
			continue
		}
		for name, i := range c.Indexes {
			if i.Relation != m.Relation {
				continue
			}
			relation, err := c.Relation(m.Relation)
			if err != nil {
				continue
			}
			pos, ok := relation.Position(i.Column)
			if !ok || pos >= len(m.Row) {
				continue
			}
			if tree := r.Trees[name]; tree != nil {
				tree.Add(m.Row[pos], m.Key)
			}
		}
	}
}
