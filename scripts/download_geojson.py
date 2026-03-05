"""Download and simplify US county boundaries from Census TIGER/Line."""

import json
import requests
import sys

URL = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"


def download_counties(output_path: str):
    print(f"Downloading county GeoJSON from {URL}...")
    response = requests.get(URL, timeout=120)
    response.raise_for_status()

    geojson = response.json()
    print(f"Downloaded {len(geojson['features'])} county features")

    for feature in geojson["features"]:
        props = feature.get("properties", {})
        if "GEO_ID" in props and "GEOID" not in props:
            props["GEOID"] = props["GEO_ID"].split("US")[-1]

    with open(output_path, "w") as f:
        json.dump(geojson, f)

    print(f"Saved to {output_path}")


if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else "data/county_geojson/us-counties.json"
    download_counties(output)
