"""
Volunteer ↔ Business Matcher
=============================
Reads volunteer survey data, geocodes UK postcodes, searches for nearby
businesses via the free Overpass API (OpenStreetMap), and exports matches
to CSV and optionally Google Sheets.
"""

import logging
import re
import time
from pathlib import Path

import pandas as pd
import requests
from geopy.distance import geodesic

from config import (
    COLUMN_RENAME_MAP,
    COLUMNS_TO_KEEP,
    DEFAULT_RADIUS_MILES,
    INDUSTRY_OSM_MAP,
    MILES_TO_METERS,
    OUTPUT_CSV_PATH,
    INPUT_CSV_PATH,
    OVERPASS_API_URL,
    OVERPASS_RATE_LIMIT_SECONDS,
    OVERPASS_TIMEOUT,
    MAX_RESULTS_PER_INDUSTRY,
    POSTCODES_IO_BULK_URL,
    GOOGLE_SHEETS_CREDENTIALS_FILE,
    GOOGLE_SHEETS_SPREADSHEET_NAME,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phase 1: Data Loading & Preprocessing
# ---------------------------------------------------------------------------

def load_volunteers(csv_path: str = INPUT_CSV_PATH) -> pd.DataFrame:
    """Load the volunteer survey CSV and return a cleaned DataFrame.

    Renames raw survey columns to clean names, parses radius and industries.
    """
    df = pd.read_csv(csv_path)
    logger.info("Loaded %d volunteers from %s", len(df), csv_path)

    # Rename columns that exist in the mapping
    rename = {k: v for k, v in COLUMN_RENAME_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Keep only the columns we care about (drop extras silently)
    cols = [c for c in COLUMNS_TO_KEEP if c in df.columns]
    df = df[cols].copy()

    # Add a stable volunteer_id
    df.insert(0, "volunteer_id", range(1, len(df) + 1))

    # Clean postcode
    df["postcode"] = df["postcode"].apply(normalize_postcode)

    # Parse radius
    df["radius_miles"] = df["radius_miles"].apply(parse_radius)

    # Parse industries into lists
    df["target_industries"] = df["target_industries"].apply(parse_industries)

    return df


def normalize_postcode(raw: str) -> str:
    """Normalize a UK postcode by uppercasing and inserting a space.

    UK postcodes always end with a digit + two letters (the inward code).
    E.g. 'M15AA' → 'M1 5AA', 'sw1a1aa' → 'SW1A 1AA'.
    """
    pc = re.sub(r"\s+", "", str(raw).strip().upper())
    # Insert space before the last 3 characters (inward code)
    return re.sub(r"^(.+?)(\d\w{2})$", r"\1 \2", pc)


def parse_radius(raw) -> int:
    """Extract the numeric radius from strings like '3 miles', '5–10 miles'.

    Takes the first number found; defaults to DEFAULT_RADIUS_MILES.
    """
    match = re.search(r"(\d+)", str(raw))
    return int(match.group(1)) if match else DEFAULT_RADIUS_MILES


def parse_industries(raw) -> list[str]:
    """Split a comma-separated industry string into a trimmed list."""
    if pd.isna(raw) or not str(raw).strip():
        return []
    return [item.strip() for item in str(raw).split(",") if item.strip()]


# ---------------------------------------------------------------------------
# Phase 2: Geocoding via postcodes.io
# ---------------------------------------------------------------------------

def geocode_postcodes(postcodes: list[str]) -> dict[str, tuple[float, float]]:
    """Bulk-geocode UK postcodes using postcodes.io (free, no auth).

    Returns a dict mapping postcode → (latitude, longitude).
    Invalid postcodes are logged and omitted.
    """
    unique_postcodes = list(set(postcodes))
    coords: dict[str, tuple[float, float]] = {}

    # postcodes.io accepts max 100 per bulk request
    for i in range(0, len(unique_postcodes), 100):
        batch = unique_postcodes[i : i + 100]
        resp = requests.post(
            POSTCODES_IO_BULK_URL,
            json={"postcodes": batch},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("result", []):
            query_pc = item["query"]
            result = item["result"]
            if result is None:
                logger.warning("Invalid postcode skipped: %s", query_pc)
                continue
            coords[query_pc] = (result["latitude"], result["longitude"])

    logger.info("Geocoded %d / %d unique postcodes", len(coords), len(unique_postcodes))
    return coords


# ---------------------------------------------------------------------------
# Phase 3: Business Search via Overpass API (OpenStreetMap)
# ---------------------------------------------------------------------------

def build_overpass_query(
    lat: float,
    lon: float,
    radius_m: float,
    osm_tags: list[str],
) -> str:
    """Build an Overpass QL query for nodes/ways matching tags within a radius.

    Args:
        lat: Centre latitude.
        lon: Centre longitude.
        radius_m: Search radius in meters.
        osm_tags: List of 'key=value' or 'key=*' strings.
    """
    unions: list[str] = []
    for tag in osm_tags:
        key, value = tag.split("=", 1)
        if value == "*":
            filt = f'["{key}"]'
        else:
            filt = f'["{key}"="{value}"]'
        unions.append(f"  node{filt}(around:{radius_m},{lat},{lon});")
        unions.append(f"  way{filt}(around:{radius_m},{lat},{lon});")

    body = "\n".join(unions)
    return (
        f"[out:json][timeout:{OVERPASS_TIMEOUT}];\n"
        f"(\n{body}\n);\n"
        f"out center body {MAX_RESULTS_PER_INDUSTRY};"
    )


def query_overpass(query: str) -> list[dict]:
    """Execute an Overpass query and return the list of elements."""
    resp = requests.post(
        OVERPASS_API_URL,
        data={"data": query},
        timeout=OVERPASS_TIMEOUT + 10,
    )
    resp.raise_for_status()
    return resp.json().get("elements", [])


def extract_business_info(element: dict) -> dict | None:
    """Extract business info from an Overpass element.

    Returns None if the element has no name tag.
    """
    tags = element.get("tags", {})
    name = tags.get("name")
    if not name:
        return None

    # For ways, coordinates come from "center"
    lat = element.get("lat") or element.get("center", {}).get("lat")
    lon = element.get("lon") or element.get("center", {}).get("lon")
    if lat is None or lon is None:
        return None

    # Build address from addr:* tags
    addr_parts = []
    for part in ["addr:housenumber", "addr:street", "addr:city", "addr:postcode"]:
        val = tags.get(part)
        if val:
            addr_parts.append(val)
    address = ", ".join(addr_parts) if addr_parts else ""

    # Determine business type from amenity / shop / leisure / office tags
    btype = (
        tags.get("amenity")
        or tags.get("shop")
        or tags.get("leisure")
        or tags.get("office")
        or tags.get("healthcare")
        or ""
    )

    return {
        "business_name": name,
        "business_address": address,
        "business_type": btype,
        "latitude": lat,
        "longitude": lon,
        "osm_id": f"{element['type']}/{element['id']}",
    }


def search_businesses(
    coords: dict[str, tuple[float, float]],
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Search for businesses near each volunteer using the Overpass API.

    Deduplicates API calls when volunteers share the same postcode/radius/industry.
    Returns a DataFrame of all (volunteer, business) matches.
    """
    # Build unique query keys to avoid duplicate API calls
    # Key: (postcode, radius_miles, frozenset(osm_tags))
    cache: dict[tuple, list[dict]] = {}
    all_matches: list[dict] = []

    for _, row in df.iterrows():
        pc = row["postcode"]
        if pc not in coords:
            logger.warning(
                "Skipping volunteer %s — postcode %s not geocoded",
                row["volunteer_name"],
                pc,
            )
            continue

        lat, lon = coords[pc]
        radius_m = row["radius_miles"] * MILES_TO_METERS

        # Resolve OSM tags for this volunteer's industries
        industries = row["target_industries"]
        osm_tags: list[str] = []
        for industry in industries:
            matched = False
            for config_key, tags in INDUSTRY_OSM_MAP.items():
                # Fuzzy match: check if the volunteer's industry text is a
                # substring of (or equals) the config key, or vice-versa
                if (
                    industry.lower() in config_key.lower()
                    or config_key.lower() in industry.lower()
                ):
                    osm_tags.extend(tags)
                    matched = True
                    break
            if not matched:
                logger.warning("No OSM mapping for industry: %s", industry)

        if not osm_tags:
            logger.warning(
                "No OSM tags resolved for volunteer %s", row["volunteer_name"]
            )
            continue

        osm_tags = list(dict.fromkeys(osm_tags))  # deduplicate, preserve order
        cache_key = (pc, row["radius_miles"], frozenset(osm_tags))

        if cache_key not in cache:
            query = build_overpass_query(lat, lon, radius_m, osm_tags)
            logger.info(
                "Querying Overpass for %s (%.0fm, %d tag groups)…",
                pc,
                radius_m,
                len(osm_tags),
            )
            try:
                elements = query_overpass(query)
            except requests.RequestException as exc:
                logger.error("Overpass query failed for %s: %s", pc, exc)
                elements = []
            time.sleep(OVERPASS_RATE_LIMIT_SECONDS)

            businesses: list[dict] = []
            for el in elements:
                info = extract_business_info(el)
                if info:
                    businesses.append(info)
            cache[cache_key] = businesses
            logger.info("  → Found %d named businesses", len(businesses))
        else:
            businesses = cache[cache_key]
            logger.info("Cache hit for %s", pc)

        # Calculate exact distance and filter
        for biz in businesses:
            dist_miles = geodesic(
                (lat, lon), (biz["latitude"], biz["longitude"])
            ).miles
            if dist_miles <= row["radius_miles"]:
                all_matches.append(
                    {
                        "volunteer_id": row["volunteer_id"],
                        "volunteer_name": row["volunteer_name"],
                        **biz,
                        "distance_miles": round(dist_miles, 2),
                    }
                )

    result = pd.DataFrame(all_matches)
    if not result.empty:
        result = result.sort_values(
            ["volunteer_id", "distance_miles"]
        ).reset_index(drop=True)
    logger.info("Total matches: %d", len(result))
    return result


# ---------------------------------------------------------------------------
# Phase 5: Export
# ---------------------------------------------------------------------------

def export_csv(df: pd.DataFrame, path: str = OUTPUT_CSV_PATH) -> None:
    """Save the matches DataFrame to a CSV file."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.info("Saved %d rows to %s", len(df), path)


def export_google_sheets(df: pd.DataFrame) -> None:
    """Push the matches DataFrame to a Google Sheet.

    Requires a service-account credentials JSON file and the google-auth +
    gspread packages.  If credentials are missing, logs a warning and skips.
    """
    creds_path = Path(GOOGLE_SHEETS_CREDENTIALS_FILE)
    if not creds_path.exists():
        logger.warning(
            "Google Sheets credentials not found at %s — skipping Sheets export. "
            "See README for setup instructions.",
            creds_path,
        )
        return

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
        gc = gspread.authorize(creds)

        try:
            sh = gc.open(GOOGLE_SHEETS_SPREADSHEET_NAME)
        except gspread.SpreadsheetNotFound:
            sh = gc.create(GOOGLE_SHEETS_SPREADSHEET_NAME)
            logger.info("Created new spreadsheet: %s", GOOGLE_SHEETS_SPREADSHEET_NAME)

        worksheet = sh.sheet1
        worksheet.clear()
        worksheet.update(
            [df.columns.tolist()] + df.astype(str).values.tolist()
        )
        logger.info(
            "Exported %d rows to Google Sheet '%s'",
            len(df),
            GOOGLE_SHEETS_SPREADSHEET_NAME,
        )
    except Exception as exc:
        logger.error("Google Sheets export failed: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the full volunteer ↔ business matching pipeline."""
    # 1. Load & preprocess
    df = load_volunteers()
    logger.info("Columns: %s", list(df.columns))
    logger.info("Sample:\n%s", df.head(2).to_string())

    # 2. Geocode
    coords = geocode_postcodes(df["postcode"].tolist())

    # 3. Search & match
    matches = search_businesses(coords, df)

    if matches.empty:
        logger.warning("No matches found. Exiting.")
        return

    # 4. Export
    export_csv(matches)
    export_google_sheets(matches)

    logger.info("Done! %d matches written.", len(matches))


if __name__ == "__main__":
    main()
