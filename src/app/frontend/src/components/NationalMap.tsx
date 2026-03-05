import { useState, useMemo } from "react";
import { MapboxOverlay } from "@deck.gl/mapbox";
import { GeoJsonLayer } from "@deck.gl/layers";
import MapGL, { useControl, NavigationControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";

const INITIAL_VIEW = {
  longitude: -98.5,
  latitude: 39.8,
  zoom: 4,
  pitch: 0,
  bearing: 0,
};

const TIER_COLORS: Record<string, [number, number, number, number]> = {
  A: [21, 128, 61, 220],
  B: [74, 175, 100, 200],
  C: [230, 168, 23, 200],
  D: [224, 112, 32, 200],
  F: [196, 30, 58, 180],
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

function DeckGLOverlay(props: { layers: any[] }) {
  const overlay = useControl(() => new MapboxOverlay({ interleaved: false }));
  overlay.setProps({ layers: props.layers });
  return null;
}

interface Props {
  geojson: any | null;
  onSelectCounty: (fips: string) => void;
}

export function NationalMap({ geojson, onSelectCounty }: Props) {
  const [hovered, setHovered] = useState<any>(null);
  const [stateFilter, setStateFilter] = useState<string>("");
  const [tierFilter, setTierFilter] = useState<string>("");
  const [minScore, setMinScore] = useState<number>(0);

  const states = useMemo(() => {
    if (!geojson) return [];
    const seen = new Set<string>();
    for (const f of geojson.features || []) {
      const st = f.properties?.STATE;
      if (st && STATE_FIPS[st]) seen.add(st);
    }
    return Array.from(seen)
      .sort((a, b) => (STATE_FIPS[a] || "").localeCompare(STATE_FIPS[b] || ""));
  }, [geojson]);

  const layers = useMemo(() => {
    if (!geojson) return [];
    return [
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
    ];
  }, [geojson, stateFilter, tierFilter, minScore, onSelectCounty]);

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

      <MapGL
        initialViewState={INITIAL_VIEW}
        style={{ width: "100%", height: "100%" }}
        mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
      >
        <DeckGLOverlay layers={layers} />
        <NavigationControl position="bottom-right" />
      </MapGL>

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
