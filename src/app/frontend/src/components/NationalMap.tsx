import { useEffect, useState, useCallback } from "react";
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

            if (stateFilter && props.STATE !== stateFilter) return [200, 200, 200, 50];
            if (tierFilter && tier !== tierFilter) return [200, 200, 200, 50];
            if (score < minScore) return [200, 200, 200, 50];

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
