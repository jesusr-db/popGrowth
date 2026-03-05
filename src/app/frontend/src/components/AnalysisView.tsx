import { useState, useMemo } from "react";
import { MapboxOverlay } from "@deck.gl/mapbox";
import { GeoJsonLayer } from "@deck.gl/layers";
import MapGL, { useControl, NavigationControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { FilterPanel } from "./FilterPanel";
import { ResultsList } from "./ResultsList";
import { CountyDetail } from "./CountyDetail";
import { Filters, DataBounds, computeBounds, defaultFilters, filterFeatures } from "./analysisFilters";

const INITIAL_VIEW = {
  longitude: -98.5, latitude: 39.8, zoom: 4, pitch: 0, bearing: 0,
};

const TIER_COLORS: Record<string, [number, number, number, number]> = {
  A: [21, 128, 61, 220],
  B: [74, 175, 100, 200],
  C: [230, 168, 23, 200],
  D: [224, 112, 32, 200],
  F: [196, 30, 58, 180],
};

const DIM_COLOR: [number, number, number, number] = [200, 200, 200, 50];

function DeckOverlay(props: { layers: any[] }) {
  const overlay = useControl(() => new MapboxOverlay({ interleaved: false }));
  overlay.setProps({ layers: props.layers });
  return null;
}

interface Props {
  geojson: any | null;
  onSelectCounty: (fips: string) => void;
  selectedFips: string | null;
  onCloseDetail: () => void;
}

export function AnalysisView({ geojson, onSelectCounty, selectedFips, onCloseDetail }: Props) {
  const features = geojson?.features || [];

  const bounds = useMemo<DataBounds>(() => computeBounds(features), [features]);
  const [filters, setFilters] = useState<Filters>(() => defaultFilters(bounds));

  const states = useMemo(() => {
    const seen = new Set<string>();
    for (const f of features) {
      const st = f.properties?.STATE;
      if (st) seen.add(st);
    }
    return Array.from(seen).sort();
  }, [features]);

  const matchingFips = useMemo(() => filterFeatures(features, filters), [features, filters]);

  const [hovered, setHovered] = useState<any>(null);

  const layers = useMemo(() => {
    if (!geojson) return [];
    return [
      new GeoJsonLayer({
        id: "analysis-counties",
        data: geojson,
        filled: true,
        stroked: true,
        getLineColor: [100, 100, 100, 80],
        lineWidthMinPixels: 0.5,
        getFillColor: (f: any) => {
          const fips = f.properties?.GEOID;
          if (!fips || !matchingFips.has(fips)) return DIM_COLOR;
          const tier = f.properties?.score_tier || "F";
          return TIER_COLORS[tier] || TIER_COLORS.F;
        },
        pickable: true,
        onHover: (info: any) => setHovered(info.object ? info : null),
        onClick: (info: any) => {
          const fips = info.object?.properties?.GEOID;
          if (fips) onSelectCounty(fips);
        },
        updateTriggers: {
          getFillColor: [matchingFips],
        },
      }),
    ];
  }, [geojson, matchingFips, onSelectCounty]);

  if (!geojson) return <div className="loading">Loading data...</div>;

  return (
    <div className="analysis-view">
      <FilterPanel
        filters={filters}
        bounds={bounds}
        matchCount={matchingFips.size}
        totalCount={features.length}
        states={states}
        onChange={setFilters}
      />

      <div className="analysis-map">
        <MapGL
          initialViewState={INITIAL_VIEW}
          style={{ width: "100%", height: "100%" }}
          mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
        >
          <DeckOverlay layers={layers} />
          <NavigationControl position="bottom-right" />
        </MapGL>

        {hovered?.object && (
          <div className="tooltip" style={{ left: hovered.x + 10, top: hovered.y + 10 }}>
            <strong>{hovered.object.properties.NAME}</strong><br />
            Score: {hovered.object.properties.composite_score?.toFixed(1) ?? "N/A"}<br />
            Tier: {hovered.object.properties.score_tier ?? "N/A"}
          </div>
        )}
      </div>

      <div className="analysis-right">
        {selectedFips ? (
          <CountyDetail fips={selectedFips} onClose={onCloseDetail} />
        ) : (
          <ResultsList
            features={features}
            matchingFips={matchingFips}
            onSelect={onSelectCounty}
          />
        )}
      </div>
    </div>
  );
}
