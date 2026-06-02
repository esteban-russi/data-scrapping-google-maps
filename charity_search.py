"""
Charity Search via FindThatCharity
====================================
Searches the FindThatCharity reconciliation API for registered charities,
CICs, and community organisations by keyword, then filters results by
distance from a given postcode.
"""

import logging

import pandas as pd
import requests
from geopy.distance import geodesic

from config import (
    CHARITY_SEARCH_TERMS,
    FINDTHATCHARITY_MAX_RESULTS,
    FINDTHATCHARITY_ORG_URL,
    FINDTHATCHARITY_RECONCILE_URL,
    POSTCODES_IO_BULK_URL,
)

logger = logging.getLogger(__name__)


def _geocode_single(postcode: str) -> tuple[float, float] | None:
    """Geocode a single UK postcode via postcodes.io."""
    resp = requests.post(
        POSTCODES_IO_BULK_URL,
        json={"postcodes": [postcode]},
        timeout=15,
    )
    resp.raise_for_status()
    results = resp.json().get("result", [])
    if results and results[0]["result"]:
        r = results[0]["result"]
        return (r["latitude"], r["longitude"])
    return None


def _reconcile_search(query: str, limit: int = FINDTHATCHARITY_MAX_RESULTS) -> list[dict]:
    """Search FindThatCharity reconciliation API for a query string."""
    params = {"queries": f'{{"q0":{{"query":"{query}","limit":{limit}}}}}'}
    resp = requests.get(
        FINDTHATCHARITY_RECONCILE_URL,
        params=params,
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("q0", {}).get("result", [])


def _fetch_org_detail(org_id: str) -> dict | None:
    """Fetch full details for a single organisation from FindThatCharity."""
    url = FINDTHATCHARITY_ORG_URL.format(org_id=org_id)
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        logger.warning("Failed to fetch detail for %s: %s", org_id, exc)
        return None


def search_charities(
    postcode: str,
    radius_miles: int,
    categories: list[str],
) -> pd.DataFrame:
    """Search for charities/CICs near a postcode by category keywords.

    Uses the FindThatCharity reconciliation API to find organisations
    matching keyword terms for each category, fetches their registered
    address postcode, geocodes it, and filters by distance.

    Args:
        postcode: UK postcode to search around.
        radius_miles: Maximum distance in miles.
        categories: Category names from CHARITY_SEARCH_TERMS.

    Returns:
        DataFrame of matching charities with name, description, address,
        type, distance, and links.
    """
    # Geocode the search centre
    centre = _geocode_single(postcode)
    if not centre:
        logger.error("Could not geocode postcode: %s", postcode)
        return pd.DataFrame()

    # Collect search terms for requested categories
    search_terms: list[str] = []
    for cat in categories:
        matched = False
        for config_key, terms in CHARITY_SEARCH_TERMS.items():
            if (
                cat.lower() in config_key.lower()
                or config_key.lower() in cat.lower()
            ):
                search_terms.extend(terms)
                matched = True
                break
        if not matched:
            logger.warning("No charity search terms for category: %s", cat)

    if not search_terms:
        logger.error("No search terms resolved for categories: %s", categories)
        return pd.DataFrame()

    search_terms = list(dict.fromkeys(search_terms))  # deduplicate

    # Search for each term and collect unique org IDs
    seen_ids: set[str] = set()
    candidates: list[dict] = []

    for term in search_terms:
        logger.info("Searching charities for: %s", term)
        results = _reconcile_search(term)
        for r in results:
            org_id = r["id"]
            if org_id in seen_ids:
                continue
            seen_ids.add(org_id)
            candidates.append({
                "org_id": org_id,
                "charity_name": r["name"].split(" (")[0],  # strip ID suffix
                "org_types": [t["name"] for t in r.get("type", [])],
                "score": r.get("score", 0),
            })

    logger.info("Found %d unique charity candidates", len(candidates))

    # Fetch details and filter by distance
    results: list[dict] = []
    postcodes_to_geocode: dict[str, list[dict]] = {}

    for cand in candidates:
        detail = _fetch_org_detail(cand["org_id"])
        if not detail:
            continue

        address = detail.get("address", {})
        charity_pc = address.get("postalCode", "")
        if not charity_pc:
            continue

        cand["description"] = detail.get("description", "")
        cand["address"] = ", ".join(
            filter(None, [
                address.get("streetAddress", ""),
                address.get("addressLocality", ""),
                charity_pc,
            ])
        )
        cand["url"] = detail.get("url", "")
        cand["postcode"] = charity_pc
        cand["active"] = detail.get("active", False)
        cand["latest_income"] = detail.get("latestIncome")

        # Group by postcode for bulk geocoding
        postcodes_to_geocode.setdefault(charity_pc, []).append(cand)

    # Bulk geocode all charity postcodes
    all_postcodes = list(postcodes_to_geocode.keys())
    charity_coords: dict[str, tuple[float, float]] = {}

    for i in range(0, len(all_postcodes), 100):
        batch = all_postcodes[i : i + 100]
        try:
            resp = requests.post(
                POSTCODES_IO_BULK_URL,
                json={"postcodes": batch},
                timeout=15,
            )
            resp.raise_for_status()
            for item in resp.json().get("result", []):
                if item["result"]:
                    charity_coords[item["query"]] = (
                        item["result"]["latitude"],
                        item["result"]["longitude"],
                    )
        except requests.RequestException as exc:
            logger.warning("Geocoding batch failed: %s", exc)

    # Filter by distance
    for pc, cands in postcodes_to_geocode.items():
        if pc not in charity_coords:
            continue
        coord = charity_coords[pc]
        dist = geodesic(centre, coord).miles
        if dist <= radius_miles:
            for cand in cands:
                results.append({
                    "charity_name": cand["charity_name"],
                    "org_id": cand["org_id"],
                    "description": cand["description"],
                    "address": cand["address"],
                    "org_types": ", ".join(cand["org_types"]),
                    "active": cand["active"],
                    "url": cand["url"],
                    "latest_income": cand["latest_income"],
                    "latitude": coord[0],
                    "longitude": coord[1],
                    "distance_miles": round(dist, 2),
                })

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values("distance_miles").reset_index(drop=True)
    logger.info(
        "Found %d charities within %d miles of %s",
        len(df), radius_miles, postcode,
    )
    return df
