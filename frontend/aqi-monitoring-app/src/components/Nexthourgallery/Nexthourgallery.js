import React, { useState } from 'react';
import NextHourCard from '../Nexthourcard/Nexthourcard';
import './style.css';

/**
 * NextHourGallery
 * Props:
 *   forecast  {Array<{ hour: number, aqi: number }>} — list of forecast entries
 */
export default function NextHourGallery({ forecast }) {
  const [activeIndex, setActiveIndex] = useState(0);

  return (
    <div className="gallery-wrapper">
      <div className="section-label">⊙ Hourly AQI Forecast</div>
      <div
        className="forecast-scroll"
        role="list"
        aria-label="Hourly AQI forecast cards"
      >
        {forecast.map((entry, i) => (
          <div key={entry.hour} role="listitem">
            <NextHourCard
              hour={entry.hour}
              aqi={entry.aqi}
              active={i === activeIndex}
              onClick={() => setActiveIndex(i)}
            />
          </div>
        ))}
      </div>
    </div>
  );
}