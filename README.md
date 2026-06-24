# Business Search

Search for businesses near a UK postcode using OpenStreetMap (Overpass API).

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

Save results to a **CSV file**:

```bash
python main.py search -p "SW1A 1AA" -r 3 --output results.csv
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
| `--output` | `-o` | Output CSV path (prints to console if omitted) |
