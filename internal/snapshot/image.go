package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"lamina/internal/catalog"
	"lamina/internal/versions"
	"os"
)

const MaxImage = 128 << 20

// Format 2 images store rows as positional tuples bound to the catalog column
// order. Format 1 images (name-keyed rows) are read-only migration inputs.
const (
	FormatLegacyRows = 1
	FormatTuples     = 2
)

type Image struct {
	Format   int              `json:"format"`
	Sequence uint64           `json:"sequence"`
	Horizon  uint64           `json:"horizon"`
	Catalog  *catalog.Catalog `json:"catalog"`
	Store    *versions.Store  `json:"store"`
}

type legacyImage struct {
	Format   int              `json:"format"`
	Sequence uint64           `json:"sequence"`
	Horizon  uint64           `json:"horizon"`
	Catalog  *catalog.Catalog `json:"catalog"`
	Store    json.RawMessage  `json:"store"`
}

type legacyTables struct {
	Tables map[string]json.RawMessage `json:"tables"`
}

type Envelope struct {
	Format  int             `json:"format"`
	Digest  string          `json:"sha256"`
	Payload json.RawMessage `json:"payload"`
}

// Encode always emits the current format. A new directory or backup is never
// produced in the legacy name-keyed shape.
func Encode(image Image) ([]byte, error) {
	image.Format = FormatTuples
	b, err := json.Marshal(image)
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(b)
	return json.Marshal(Envelope{Format: FormatTuples, Digest: hex.EncodeToString(h[:]), Payload: b})
}

// Decode accepts both envelope generations. A legacy payload is converted and
// validated against its own catalog before it is returned, and the second
// result reports that the caller must publish the converted materials.
func Decode(b []byte) (Image, bool, error) {
	var image Image
	var env Envelope
	if len(b) > MaxImage {
		return image, false, fmt.Errorf("image exceeds limit")
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return image, false, err
	}
	if env.Format != FormatLegacyRows && env.Format != FormatTuples {
		return image, false, fmt.Errorf("image format or digest mismatch")
	}
	h := sha256.Sum256(env.Payload)
	if hex.EncodeToString(h[:]) != env.Digest {
		return image, false, fmt.Errorf("image format or digest mismatch")
	}
	var probe struct {
		Format int `json:"format"`
	}
	if err := json.Unmarshal(env.Payload, &probe); err != nil {
		return image, false, err
	}
	if probe.Format != env.Format {
		return image, false, fmt.Errorf("image format or digest mismatch")
	}
	if probe.Format == FormatLegacyRows {
		var legacy legacyImage
		if err := json.Unmarshal(env.Payload, &legacy); err != nil {
			return image, false, err
		}
		if legacy.Catalog == nil {
			return image, false, fmt.Errorf("invalid image header")
		}
		var tables legacyTables
		if err := json.Unmarshal(legacy.Store, &tables); err != nil {
			return image, false, err
		}
		if tables.Tables == nil {
			return image, false, fmt.Errorf("legacy store missing tables")
		}
		store := versions.New()
		// Convert one relation at a time and release its legacy rows, so the
		// conversion never needs two whole databases resident.
		for name, raw := range tables.Tables {
			var rows map[string][]versions.LegacyVersion
			if err := json.Unmarshal(raw, &rows); err != nil {
				return image, false, err
			}
			converted, err := versions.ConvertTable(name, rows, legacy.Catalog)
			if err != nil {
				return image, false, err
			}
			store.Tables[name] = converted
			delete(tables.Tables, name)
		}
		image = Image{Format: FormatTuples, Sequence: legacy.Sequence, Horizon: legacy.Horizon, Catalog: legacy.Catalog, Store: store}
		if err := Validate(image); err != nil {
			return Image{}, false, err
		}
		return image, true, nil
	}
	if err := json.Unmarshal(env.Payload, &image); err != nil {
		return image, false, err
	}
	if err := Validate(image); err != nil {
		return image, false, err
	}
	return image, false, nil
}

// Read returns the image and whether the source file was in the legacy shape.
func Read(path string) (Image, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return Image{}, false, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxImage+1))
	if err != nil {
		return Image{}, false, err
	}
	return Decode(b)
}

func Validate(image Image) error {
	if image.Format != FormatTuples || image.Horizon > image.Sequence || image.Catalog == nil || image.Store == nil || image.Catalog.Relations == nil || image.Catalog.Indexes == nil || image.Store.Tables == nil {
		return fmt.Errorf("invalid image header")
	}
	for name, r := range image.Catalog.Relations {
		if name != r.Name || r.Created == 0 || r.Created > image.Sequence {
			return fmt.Errorf("invalid catalog sequence")
		}
		if err := r.Validate(); err != nil {
			return err
		}
	}
	for name, definition := range image.Catalog.Indexes {
		relation, ok := image.Catalog.Relations[definition.Relation]
		if !ok || name != definition.Name || !catalog.ValidName(name) || definition.Created < relation.Created || definition.Created > image.Sequence {
			return fmt.Errorf("invalid index metadata")
		}
		if _, ok := relation.Column(definition.Column); !ok {
			return fmt.Errorf("invalid index column")
		}
	}
	for name, rows := range image.Store.Tables {
		r, ok := image.Catalog.Relations[name]
		if !ok {
			return fmt.Errorf("image references unknown relation")
		}
		for key, chain := range rows {
			var last uint64
			if len(chain) == 0 {
				return fmt.Errorf("empty version chain")
			}
			for _, v := range chain {
				if v.Sequence <= last || v.Sequence > image.Sequence || v.Sequence <= r.Created {
					return fmt.Errorf("invalid version order")
				}
				last = v.Sequence
				if !v.Deleted {
					if err := r.ValidateTuple(v.Row); err != nil {
						return fmt.Errorf("invalid stored tuple: %w", err)
					}
					if stored, err := r.KeyTuple(v.Row); err != nil || stored != key {
						return fmt.Errorf("invalid stored tuple")
					}
				}
			}
		}
	}
	for _, definition := range image.Catalog.Indexes {
		if !definition.Unique {
			continue
		}
		relation := image.Catalog.Relations[definition.Relation]
		pos, ok := relation.Position(definition.Column)
		if !ok {
			return fmt.Errorf("invalid index column")
		}
		seen := map[string]bool{}
		for _, chain := range image.Store.Tables[definition.Relation] {
			version, exists := versions.Visible(chain, image.Sequence)
			if !exists || pos >= len(version.Row) {
				continue
			}
			value := version.Row[pos]
			if value.Null() {
				continue
			}
			if seen[value.Key()] {
				return fmt.Errorf("image violates unique index %s", definition.Name)
			}
			seen[value.Key()] = true
		}
	}
	return nil
}
