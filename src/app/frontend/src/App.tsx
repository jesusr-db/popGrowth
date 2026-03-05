import { useState } from "react";
import { NationalMap } from "./components/NationalMap";
import { CountyDetail } from "./components/CountyDetail";
import { WeightTuner } from "./components/WeightTuner";
import "./App.css";

export default function App() {
  const [selectedFips, setSelectedFips] = useState<string | null>(null);
  const [showWeightTuner, setShowWeightTuner] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);

  return (
    <div className="app">
      <header className="app-header">
        <h1>Store Siting — Growth Score Explorer</h1>
        <button onClick={() => setShowWeightTuner(!showWeightTuner)}>
          {showWeightTuner ? "Hide Weights" : "Adjust Weights"}
        </button>
      </header>

      <div className="app-body">
        <div className="map-container">
          <NationalMap
            onSelectCounty={setSelectedFips}
            refreshKey={refreshKey}
          />
        </div>

        {selectedFips && (
          <div className="detail-panel">
            <CountyDetail
              fips={selectedFips}
              onClose={() => setSelectedFips(null)}
            />
          </div>
        )}

        {showWeightTuner && (
          <div className="weight-panel">
            <WeightTuner onRecalculate={() => setRefreshKey((k) => k + 1)} />
          </div>
        )}
      </div>
    </div>
  );
}
