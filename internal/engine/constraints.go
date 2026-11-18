package engine

import (
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/index"
	"lamina/internal/journal"
)

func (d *DB) unique(table string, writes map[string]map[string]staged) error {
	rows := d.store.Scan(table, d.sequence)
	overlay(rows, writes, table)
	relation, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	for _, i := range d.catalog.Indexes {
		if i.Relation == table && i.Unique {
			pos, ok := relation.Position(i.Column)
			if !ok {
				return fault("invalid", "unique index %s references unknown column", i.Name)
			}
			if err := checkUnique(rows, i, pos); err != nil {
				return err
			}
		}
	}
	return nil
}
func checkUnique(rows map[string]datum.Tuple, i catalog.Index, pos int) error {
	values := map[string]string{}
	for key, row := range rows {
		if pos >= len(row) {
			continue
		}
		v := row[pos]
		if v.Null() {
			continue
		}
		token := v.Key()
		if prior, exists := values[token]; exists && prior != key {
			return fault("conflict", "unique constraint %s violated", i.Name)
		}
		values[token] = key
	}
	return nil
}
func (d *DB) CreateRelation(r catalog.Relation) (catalog.Relation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return r, err
	}
	if err := r.Validate(); err != nil {
		return r, invalid(err)
	}
	if _, exists := d.catalog.Relations[r.Name]; exists {
		return r, fault("conflict", "relation exists")
	}
	r.Created = d.sequence + 1
	if err := d.publish(journal.Record{Kind: "relation", Relation: &r}); err != nil {
		return r, err
	}
	return r, nil
}
func (d *DB) CreateIndex(i catalog.Index) (catalog.Index, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return i, err
	}
	if err := d.catalog.CheckIndex(i); err != nil {
		return i, invalid(err)
	}
	relation, _ := d.catalog.Relation(i.Relation)
	pos, _ := relation.Position(i.Column)
	if i.Unique {
		if err := checkUnique(d.store.Scan(i.Relation, d.sequence), i, pos); err != nil {
			return i, err
		}
	}
	i.Created = d.sequence + 1
	if err := d.publish(journal.Record{Kind: "index", Index: &i}); err != nil {
		return i, err
	}
	d.indexes = index.Build(d.catalog, d.store)
	return i, nil
}
