import React, { useRef, useState } from 'react';
import { aqiColor, aqiCategory } from '../../utils/Aqihelpers';
import './style.css';

const LEGEND = [
  { label: 'Good (0–50)',           color: '#22c55e' },
  { label: 'Moderate (51–100)',     color: '#eab308' },
  { label: 'Sensitive (101–150)',   color: '#f97316' },
  { label: 'Unhealthy (151–200)',   color: '#ef4444' },
  { label: 'Very Unhealthy (201+)', color: '#a855f7' },
];

// Grid lines for the map background
const GRID_X = [50, 130, 210, 290, 370];
const GRID_Y = [40, 120, 200, 280];

export default function Map({ locations, activeIndex, onSelectLocation }) {
  const [tooltip, setTooltip] = useState({ visible: false, text: '', x: 0, y: 0 });
  const svgRef = useRef(null);

  function handleMouseEnter(loc, i) {
    setTooltip({
      visible: true,
      text: `${loc.district}  ·  AQI ${loc.aqi_final}  ·  ${loc.label}`,
      x: loc.cx + 20,
      y: loc.cy - 14,
    });
  }

  function handleMouseLeave() {
    setTooltip({ ...tooltip, visible: false });
  }

  return (
    <div className="map-panel">
      <span className="map-label">Hanoi Districts</span>

      <svg
        ref={svgRef}
        className="map-svg"
        viewBox="0 0 440 360"
        aria-label="Interactive map of Hanoi air quality districts"
      >
        {/* Grid lines */}
        {GRID_X.map(x => (
          <line key={`gx-${x}`} x1={x} y1={0} x2={x} y2={360} stroke="#1e3a5f" strokeWidth="0.5" />
        ))}
        {GRID_Y.map(y => (
          <line key={`gy-${y}`} x1={0} y1={y} x2={440} y2={y} stroke="#1e3a5f" strokeWidth="0.5" />
        ))}

        {/* Location dots */}
        {locations.map((loc, i) => {
          const col = loc.color;
          const isActive = i === activeIndex;
          return (
            <g
              key={loc.district+loc.city}
              className="map-location-group"
              onClick={() => onSelectLocation(i)}
              onMouseEnter={() => handleMouseEnter(loc, i)}
              onMouseLeave={handleMouseLeave}
              aria-label={`${loc.district}, AQI ${loc.aqi_final}`}
              role="button"
              tabIndex={0}
              onKeyDown={e => e.key === 'Enter' && onSelectLocation(i)}
            >
              {/* Pulse ring */}
              <circle
                cx={loc.lat} cy={loc.lon} r={26}
                fill={col} fillOpacity="0.1"
                stroke={col} strokeOpacity="0.3" strokeWidth="1"
              />
              {/* Main dot */}
              <circle
                cx={loc.lat} cy={loc.lon}
                r={isActive ? 18 : 15}
                fill={col} fillOpacity="0.85"
                stroke={isActive ? '#fff' : 'none'}
                strokeWidth="2.5"
                className="map-dot"
              />
              {/* AQI number */}
              <text
                x={loc.lat} y={loc.lon + 5}
                textAnchor="middle"
                fill="#fff"
                fontSize="10"
                fontFamily="Space Mono, monospace"
                fontWeight="700"
              >
                {loc.aqi_final}
              </text>
              {/* District name label */}
              <text
                x={loc.lat} y={loc.lon + 32}
                textAnchor="middle"
                fill="#64748b"
                fontSize="9"
                fontFamily="DM Sans, sans-serif"
              >
                {loc.district}
              </text>
            </g>
          );
        })}
      </svg>

      {/* Legend */}
      <div className="map-legend">
        {LEGEND.map(item => (
          <div key={item.label} className="legend-item">
            <div className="legend-dot" style={{ background: item.color }} />
            <span>{item.label}</span>
          </div>
        ))}
      </div>

      {/* Tooltip */}
      {tooltip.visible && (
        <div
          className="map-tooltip"
          style={{ left: tooltip.x, top: tooltip.y }}
        >
          {tooltip.text}
        </div>
      )}
    </div>
  );
}