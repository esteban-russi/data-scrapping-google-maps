"""Configuration constants for the Volunteer ↔ Business Matcher."""

# --- Column rename map: raw survey column → clean name ---
COLUMN_RENAME_MAP: dict[str, str] = {
    "Full Name": "volunteer_name",
    "What postcode will you be working from?": "postcode",
    "What radius are you comfortable covering?": "radius_miles",
    "Which industries are you most interested in outreaching to? "
    "(Select all that apply. Use 'Other' to specify additional interests.)": "target_industries",
    "Are there any industries you do NOT want to outreach to? (Optional)": "excluded_industries",
    "Do you have any existing contacts or relationships in these industries? (Optional)": "existing_contacts",
}

# Columns to keep after renaming
COLUMNS_TO_KEEP: list[str] = list(COLUMN_RENAME_MAP.values())

# Default radius (miles) when parsing fails
DEFAULT_RADIUS_MILES: int = 3

# --- postcodes.io ---
POSTCODES_IO_BULK_URL: str = "https://api.postcodes.io/postcodes"

# --- Overpass API ---
OVERPASS_API_URL: str = "https://overpass-api.de/api/interpreter"
OVERPASS_RATE_LIMIT_SECONDS: float = 1.0
OVERPASS_TIMEOUT: int = 30
MAX_RESULTS_PER_INDUSTRY: int = 50

# Miles ↔ Meters conversion
MILES_TO_METERS: float = 1609.344

# --- Industry → OSM tag mapping ---
# Each industry maps to a list of Overpass filter strings: "key=value" or "key=*"
INDUSTRY_OSM_MAP: dict[str, list[str]] = {
    "Hospitality (restaurants, cafés, bars)": [
        "amenity=restaurant",
        "amenity=pub",
        "amenity=bar",
    ],
    "Local Cafés & Coffee Shops": [
        "amenity=cafe",
    ],
    "Community Gyms / Local Fitness Centres": [
        "leisure=fitness_centre",
        "leisure=sports_centre",
    ],
    "Local Independent Businesses": [
        "shop=convenience",
        "shop=florist",
        "shop=bakery",
        "shop=butcher",
        "shop=greengrocer",
        "shop=hairdresser",
        "shop=beauty",
        "shop=laundry",
        "shop=bookmaker",
    ],
    "Education (colleges, training providers)": [
        "amenity=college",
        "amenity=university",
        "amenity=training",
    ],
    "Events & Venues": [
        "amenity=events_venue",
        "amenity=community_centre",
        "amenity=conference_centre",
        "leisure=dance",
    ],
    "Retail (shops, supermarkets)": [
        "shop=supermarket",
        "shop=convenience",
        "shop=clothes",
        "shop=department_store",
    ],
    "Health & Wellness": [
        "amenity=pharmacy",
        "healthcare=centre",
        "healthcare=clinic",
    ],
    "Faith & Religious Organisations": [
        "amenity=place_of_worship",
    ],
    "Corporate / Office-Based Companies": [
        "office=company",
        "office=ngo",
        "office=association",
    ],
}

# --- Google Sheets (optional) ---
GOOGLE_SHEETS_CREDENTIALS_FILE: str = "credentials.json"
GOOGLE_SHEETS_SPREADSHEET_NAME: str = "Volunteer Business Matches"

# --- Output ---
OUTPUT_CSV_PATH: str = "data/matches.csv"
INPUT_CSV_PATH: str = "data/responses.csv"
