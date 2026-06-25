# Business Search

Search for businesses near a UK postcode using OpenStreetMap data.

## How it works

This script finds real-world businesses within a given radius of any UK postcode. It uses three free, public APIs — no API keys required:

1. **postcodes.io** — Converts a UK postcode (e.g. "M1 5JG") into geographic coordinates (latitude/longitude).

2. **Overpass API (OpenStreetMap)** — Queries the OpenStreetMap database for businesses matching specific categories within a radius of those coordinates.

3. **geopy** — Calculates the precise distance (in miles) between the search centre and each business found.

### What is OpenStreetMap?

[OpenStreetMap](https://www.openstreetmap.org/) (OSM) is a collaborative, open-source map of the world — like Wikipedia for maps. Contributors tag real-world places (shops, restaurants, offices, etc.) with structured metadata. The [Overpass API](https://overpass-api.de/) lets you query this data programmatically.

### How businesses are found

Each industry category maps to one or more OSM tags. For example:

| Industry | OSM Tags Searched |
|----------|-------------------|
| Hospitality | `amenity=restaurant`, `amenity=pub`, `amenity=bar` |
| Cafés | `amenity=cafe` |
| Retail | `shop=supermarket`, `shop=clothes`, `shop=department_store` |
| Gyms / Fitness | `leisure=fitness_centre`, `leisure=sports_centre` |
| Health & Wellness | `amenity=pharmacy`, `healthcare=centre`, `healthcare=clinic` |

The script builds an Overpass QL query that searches for nodes and ways (points and buildings) with these tags within the specified radius. Results are filtered to only include places that have a name, and the exact geodesic distance is calculated to ensure they fall within the requested radius.

### Output

Results are automatically saved to the `outputs/` folder with descriptive filenames:

```
outputs/{postcode}_{radius}mi_{industry}_{timestamp}.csv
```

For example: `outputs/m15jg_1mi_hospitality_20260624_133147.csv`

Each row contains:
- `business_name` — Name of the business
- `business_address` — Address (from OSM addr:* tags, may be partial)
- `business_type` — Category (e.g. restaurant, pub, pharmacy)
- `latitude` / `longitude` — Coordinates
- `osm_id` — Unique OpenStreetMap identifier
- `distance_miles` — Distance from the search postcode

## Setup

```bash
pip install -r requirements.txt
```

## Usage

### Single postcode search

Search **all industries** near a postcode (no `--industry` flag):

```bash
python main.py search --postcode "SW1A 1AA" --radius 3
```

Filter by a **specific industry**:

```bash
python main.py search --postcode "M1 5AA" --radius 5 --industry "Hospitality"
```

Search **multiple industries**:

```bash
python main.py search -p "E1 6AN" -r 2 -i "Hospitality" -i "Retail"
```

### Batch mode (from volunteer CSV)

Run the full pipeline using the volunteer survey CSV:

```bash
python main.py batch
```

## Available industries

Partial matches work — you don't need the full name:

- Hospitality (restaurants, cafés, bars)
- Cafés / Coffee Shops
- Gyms / Fitness Centres
- Local Independent Businesses
- Education (colleges, training providers)
- Events & Venues
- Retail (shops, supermarkets)
- Health & Wellness
- Faith & Religious Organisations
- Corporate / Office-Based Companies

## Options

| Flag | Short | Description |
|------|-------|-------------|
| `--postcode` | `-p` | UK postcode to search around (required) |
| `--radius` | `-r` | Search radius in miles (default: 3) |
| `--industry` | `-i` | Industry filter (repeatable, omit for all) |

## Limitations

- Business data depends on what OSM contributors have mapped — coverage varies by area.
- Addresses may be incomplete (not all OSM entries have full address tags).
- The free Overpass API has rate limits; large queries are batched with delays to avoid throttling.
- Phone numbers, emails, and websites are not always available in OSM data.
