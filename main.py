"""
Location-Based Organisation Finder
===================================
Searches for nearby organisations (charities, CICs, community groups,
businesses) by postcode + radius + category using the Overpass API
(OpenStreetMap).  Also supports batch mode from a CSV of volunteers.

Usage:
    # Single query
    python main.py query --postcode "M1 5AA" --radius 5 --category "PTSD & Mental Health Services"

    # Batch from CSV
    python main.py csv --input data/responses.csv --output data/matches.csv

    # List available categories
    python main.py --list-categories
"""

import argparse
import logging
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from geopy.distance import geodesic

from config import (
    CHARITY_SEARCH_TERMS,
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
)
from charity_search import search_charities

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

def query_overpass(query: str, max_retries: int = 3) -> list[dict]:
    """Execute an Overpass query and return the list of elements.

    Retries with exponential backoff on transient errors (406, 429, 5xx).
    """
    headers = {"User-Agent": "HomelessHouseOrgFinder/1.0"}
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                OVERPASS_API_URL,
                data={"data": query},
                headers=headers,
                timeout=OVERPASS_TIMEOUT + 10,
            )
            resp.raise_for_status()
            return resp.json().get("elements", [])
        except requests.RequestException as exc:
            status = getattr(exc.response, "status_code", None) if hasattr(exc, "response") else None
            if status in (406, 429, 504) and attempt < max_retries - 1:
                wait = (attempt + 1) * 5
                logger.warning(
                    "Overpass returned %s, retrying in %ds (attempt %d/%d)",
                    status, wait, attempt + 1, max_retries,
                )
                time.sleep(wait)
            else:
                raise


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
        "phone": tags.get("phone") or tags.get("contact:phone") or "",
        "email": tags.get("email") or tags.get("contact:email") or "",
        "website": tags.get("website") or tags.get("contact:website") or "",
        "latitude": lat,
        "longitude": lon,
        "osm_id": f"{element['type']}/{element['id']}",
    }


# ---------------------------------------------------------------------------
# Phase 4a: Resolve categories → OSM tags
# ---------------------------------------------------------------------------

def resolve_osm_tags(categories: list[str]) -> list[str]:
    """Resolve category names to OSM tags using fuzzy matching.

    Each category is matched against INDUSTRY_OSM_MAP keys.  Unmatched
    categories are logged as warnings.
    """
    osm_tags: list[str] = []
    for category in categories:
        matched = False
        for config_key, tags in INDUSTRY_OSM_MAP.items():
            if (
                category.lower() in config_key.lower()
                or config_key.lower() in category.lower()
            ):
                osm_tags.extend(tags)
                matched = True
                break
        if not matched:
            logger.warning("No OSM mapping for category: %s", category)
    return list(dict.fromkeys(osm_tags))  # deduplicate, preserve order


# ---------------------------------------------------------------------------
# Phase 4b: Single-location search
# ---------------------------------------------------------------------------

def search_location(
    postcode: str,
    radius_miles: int,
    categories: list[str],
) -> pd.DataFrame:
    """Search for organisations near a single postcode.

    Args:
        postcode: UK postcode (e.g. 'M1 5AA').
        radius_miles: Search radius in miles.
        categories: List of category names from INDUSTRY_OSM_MAP.

    Returns:
        DataFrame of matching organisations with name, address, type,
        coordinates, and distance.
    """
    postcode = normalize_postcode(postcode)
    coords = geocode_postcodes([postcode])
    if postcode not in coords:
        logger.error("Could not geocode postcode: %s", postcode)
        return pd.DataFrame()

    lat, lon = coords[postcode]
    radius_m = radius_miles * MILES_TO_METERS

    osm_tags = resolve_osm_tags(categories)
    if not osm_tags:
        logger.error("No OSM tags resolved for categories: %s", categories)
        return pd.DataFrame()

    # Query Overpass per-tag to avoid 406 on large unions
    elements: list[dict] = []
    seen_ids: set[str] = set()
    for tag in osm_tags:
        query = build_overpass_query(lat, lon, radius_m, [tag])
        logger.info(
            "Querying Overpass for %s — tag %s (%.0fm)…",
            postcode, tag, radius_m,
        )
        try:
            batch = query_overpass(query)
            for el in batch:
                uid = f"{el['type']}/{el['id']}"
                if uid not in seen_ids:
                    seen_ids.add(uid)
                    elements.append(el)
        except requests.RequestException as exc:
            logger.error("Overpass query failed for %s [%s]: %s", postcode, tag, exc)
        time.sleep(OVERPASS_RATE_LIMIT_SECONDS)

    results: list[dict] = []
    for el in elements:
        info = extract_business_info(el)
        if not info:
            continue
        dist = geodesic((lat, lon), (info["latitude"], info["longitude"])).miles
        if dist <= radius_miles:
            info["distance_miles"] = round(dist, 2)
            results.append(info)

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values("distance_miles").reset_index(drop=True)
    logger.info("Found %d results within %d miles of %s", len(df), radius_miles, postcode)
    return df


# ---------------------------------------------------------------------------
# Phase 4c: Batch search (CSV volunteers)
# ---------------------------------------------------------------------------

def search_businesses(
    coords: dict[str, tuple[float, float]],
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Search for businesses near each volunteer using the Overpass API.

    Deduplicates API calls when volunteers share the same postcode/radius/industry.
    Returns a DataFrame of all (volunteer, business) matches.
    """
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

        osm_tags = resolve_osm_tags(row["target_industries"])
        if not osm_tags:
            logger.warning(
                "No OSM tags resolved for volunteer %s", row["volunteer_name"]
            )
            continue

        cache_key = (pc, row["radius_miles"], frozenset(osm_tags))

        if cache_key not in cache:
            # Query Overpass per-tag to avoid 406 on large unions
            elements: list[dict] = []
            seen_el_ids: set[str] = set()
            for tag in osm_tags:
                query = build_overpass_query(lat, lon, radius_m, [tag])
                logger.info(
                    "Querying Overpass for %s — tag %s (%.0fm)…",
                    pc, tag, radius_m,
                )
                try:
                    batch = query_overpass(query)
                    for el in batch:
                        uid = f"{el['type']}/{el['id']}"
                        if uid not in seen_el_ids:
                            seen_el_ids.add(uid)
                            elements.append(el)
                except requests.RequestException as exc:
                    logger.error("Overpass query failed for %s [%s]: %s", pc, tag, exc)
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



# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with 'query' and 'csv' subcommands."""
    parser = argparse.ArgumentParser(
        description="Search for nearby organisations by postcode, radius, and category.",
    )
    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="List all available search categories and exit.",
    )

    subs = parser.add_subparsers(dest="command")

    # --- query subcommand ---
    q = subs.add_parser("query", help="Search from a single postcode.")
    q.add_argument("--postcode", "-p", required=True, help="UK postcode (e.g. 'M1 5AA').")
    q.add_argument("--radius", "-r", type=int, default=DEFAULT_RADIUS_MILES, help="Radius in miles (default: %(default)s).")
    q.add_argument(
        "--category", "-c",
        action="append",
        required=True,
        help="Category to search for (repeatable). Use --list-categories to see options.",
    )
    q.add_argument("--output", "-o", default=OUTPUT_CSV_PATH, help="Output CSV path (default: %(default)s).")
    q.add_argument(
        "--source", "-s",
        choices=["osm", "charities", "both"],
        default="both",
        help="Data source: 'osm' (OpenStreetMap), 'charities' (FindThatCharity), or 'both' (default: %(default)s).",
    )

    # --- csv subcommand ---
    c = subs.add_parser("csv", help="Batch search from a volunteer CSV.")
    c.add_argument("--input", "-i", default=INPUT_CSV_PATH, help="Input CSV path (default: %(default)s).")
    c.add_argument("--output", "-o", default=OUTPUT_CSV_PATH, help="Output CSV path (default: %(default)s).")

    return parser


def list_categories() -> None:
    """Print available categories and their OSM tags."""
    print("\nAvailable categories (OSM / OpenStreetMap):")
    print("-" * 50)
    for i, (name, tags) in enumerate(INDUSTRY_OSM_MAP.items(), 1):
        print(f"  {i:2d}. {name}")
        for tag in tags:
            print(f"        └─ {tag}")

    print("\nAvailable categories (Charity Register):")
    print("-" * 50)
    for i, (name, terms) in enumerate(CHARITY_SEARCH_TERMS.items(), 1):
        print(f"  {i:2d}. {name}")
        for term in terms:
            print(f"        └─ \"{term}\"")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point: dispatch to query or csv subcommand."""
    parser = build_parser()
    args = parser.parse_args()

    if args.list_categories:
        list_categories()
        sys.exit(0)

    if args.command == "query":
        dfs: list[pd.DataFrame] = []

        if args.source in ("osm", "both"):
            osm_results = search_location(
                postcode=args.postcode,
                radius_miles=args.radius,
                categories=args.category,
            )
            if not osm_results.empty:
                osm_results.insert(0, "source", "osm")
                dfs.append(osm_results)

        if args.source in ("charities", "both"):
            charity_results = search_charities(
                postcode=args.postcode,
                radius_miles=args.radius,
                categories=args.category,
            )
            if not charity_results.empty:
                charity_results.insert(0, "source", "charity_register")
                dfs.append(charity_results)

        if not dfs:
            logger.warning("No results found.")
            sys.exit(1)

        results = pd.concat(dfs, ignore_index=True)
        export_csv(results, args.output)
        print(f"\n{len(results)} results saved to {args.output}")

        # Print summary based on available columns
        name_col = "charity_name" if "charity_name" in results.columns else "business_name"
        summary_cols = [c for c in ["source", name_col, "business_type", "org_types", "distance_miles"] if c in results.columns]
        print(results[summary_cols].to_string(index=False))

    elif args.command == "csv":
        df = load_volunteers(args.input)
        logger.info("Columns: %s", list(df.columns))
        coords = geocode_postcodes(df["postcode"].tolist())
        matches = search_businesses(coords, df)
        if matches.empty:
            logger.warning("No matches found.")
            sys.exit(1)
        export_csv(matches, args.output)
        logger.info("Done! %d matches written.", len(matches))

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
