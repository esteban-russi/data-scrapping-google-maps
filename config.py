"""Configuration constants for the Location-Based Organisation Finder."""

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
OVERPASS_RATE_LIMIT_SECONDS: float = 2.0
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
    "Charities & NGOs": [
        "office=charity",
        "office=ngo",
        "office=association",
        "office=foundation",
    ],
    "Community Groups & Centres": [
        "amenity=community_centre",
        "amenity=social_facility",
        "amenity=social_centre",
        "office=association",
    ],
    "PTSD & Mental Health Services": [
        "healthcare=psychotherapist",
        "healthcare=counselling",
        "amenity=social_facility",
        "healthcare=centre",
        "healthcare=clinic",
        "office=charity",
        "office=therapist",
    ],
    "Social Services": [
        "office=social_services",
        "amenity=social_facility",
        "office=charity",
        "office=ngo",
    ],
}

# --- Output ---
OUTPUT_CSV_PATH: str = "data/matches.csv"
INPUT_CSV_PATH: str = "data/responses.csv"

# --- FindThatCharity API (free, no auth) ---
FINDTHATCHARITY_RECONCILE_URL: str = "https://findthatcharity.uk/reconcile"
FINDTHATCHARITY_ORG_URL: str = "https://findthatcharity.uk/orgid/{org_id}.json"
FINDTHATCHARITY_MAX_RESULTS: int = 100

# Keyword groups for charity search — maps category → search terms
CHARITY_SEARCH_TERMS: dict[str, list[str]] = {
    "PTSD & Mental Health Services": [
        "PTSD",
        "post traumatic stress",
        "mental health",
        "trauma recovery",
        "counselling veterans",
        "anxiety depression",
        "psychological therapy",
        "veterans support",
        "mental wellbeing",
        "crisis support",
        "bereavement counselling",
        "emotional support",
    ],
    "Charities & NGOs": [
        "charity",
        "voluntary organisation",
    ],
    "Community Groups & Centres": [
        "community group",
        "community centre",
        "neighbourhood support",
    ],
    "Social Services": [
        "social services",
        "welfare support",
        "homelessness",
        "housing support",
    ],
    "Health & Wellness": [
        "health",
        "wellness",
        "wellbeing",
    ],
}
