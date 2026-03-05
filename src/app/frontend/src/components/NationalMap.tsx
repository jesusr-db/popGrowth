import { useEffect, useState, useMemo } from "react";
import DeckGL from "@deck.gl/react";
import { GeoJsonLayer } from "@deck.gl/layers";
import { Map } from "react-map-gl/maplibre";
import { api } from "../api/client";

const INITIAL_VIEW = {
  longitude: -98.5,
  latitude: 39.8,
  zoom: 4,
  pitch: 0,
  bearing: 0,
};

const TIER_COLORS: Record<string, [number, number, number, number]> = {
  A: [34, 139, 34, 200],
  B: [144, 238, 144, 200],
  C: [255, 215, 0, 200],
  D: [255, 140, 0, 200],
  F: [220, 20, 60, 200],
};

const STATE_FIPS: Record<string, string> = {
  "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
  "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
  "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
  "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
  "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
  "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
  "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
  "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
  "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
  "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
  "56": "WY",
};

interface Props {
  onSelectCounty: (fips: string) => void;
  refreshKey: number;
}

export function NationalMap({ onSelectCounty, refreshKey }: Props) {
  const [geojson, setGeojson] = useState<any | null>(null);
  const [hovered, setHovered] = useState<any>(null);
  const [stateFilter, setStateFilter] = useState<string>("");
  const [tierFilter, setTierFilter] = useState<string>("");
  const [minScore, setMinScore] = useState<number>(0);

  useEffect(() => {
    api.getGeoJson().then(setGeojson);
  }, [refreshKey]);

  const states = useMemo(() => {
    if (!geojson) return [];
    const seen = new Set<string>();
    for (const f of geojson.features || []) {
      const st = f.properties?.STATE;
      if (st && STATE_FIPS[st]) seen.add(st);
    }
    return Array.from(seen)
      .sort((a, b) => (STATE_FIPS[a] || "").localeCompare(STATE_FIPS[b] || ""))
  }, [geojson]);

  const layers = geojson
    ? [
        new GeoJsonLayer({
          id: "counties",
          data: geojson,
          filled: true,
          stroked: true,
          getLineColor: [100, 100, 100, 80],
          lineWidthMinPixels: 0.5,
          getFillColor: (f: any) => {
            const props = f.properties || {};
            const tier = props.score_tier || "F";
            const score = props.composite_score || 0;

            if (stateFilter && props.STATE !== stateFilter) return [200, 200, 200, 50] as [number, number, number, number];
            if (tierFilter && tier !== tierFilter) return [200, 200, 200, 50] as [number, number, number, number];
            if (score < minScore) return [200, 200, 200, 50] as [number, number, number, number];

            return TIER_COLORS[tier] || TIER_COLORS.F;
          },
          pickable: true,
          onHover: (info: any) => setHovered(info.object ? info : null),
          onClick: (info: any) => {
            const fips = info.object?.properties?.GEOID;
            if (fips) onSelectCounty(fips);
          },
          updateTriggers: {
            getFillColor: [stateFilter, tierFilter, minScore],
          },
        }),
      ]
    : [];

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      <div className="map-filters">
        <select value={stateFilter} onChange={(e) => setStateFilter(e.target.value)}>
          <option value="">All States</option>
          {states.map((st) => (
            <option key={st} value={st}>{STATE_FIPS[st]}</option>
          ))}
        </select>
        <select value={tierFilter} onChange={(e) => setTierFilter(e.target.value)}>
          <option value="">All Tiers</option>
          {["A", "B", "C", "D", "F"].map((t) => (
            <option key={t} value={t}>Tier {t}</option>
          ))}
        </select>
        <label>
          Min Score: {minScore}
          <input
            type="range" min={0} max={100} value={minScore}
            onChange={(e) => setMinScore(Number(e.target.value))}
          />
        </label>
      </div>

      <DeckGL initialViewState={INITIAL_VIEW} controller layers={layers}>
        <Map mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json" />
      </DeckGL>

      {hovered && hovered.object && (
        <div
          className="tooltip"
          style={{ left: hovered.x + 10, top: hovered.y + 10 }}
        >
          <strong>{hovered.object.properties.NAME}</strong>
          <br />
          Score: {hovered.object.properties.composite_score?.toFixed(1) ?? "N/A"}
          <br />
          Tier: {hovered.object.properties.score_tier ?? "N/A"}
          <br />
          Rank: #{hovered.object.properties.rank_national ?? "N/A"}
        </div>
      )}
    </div>
  );
}
