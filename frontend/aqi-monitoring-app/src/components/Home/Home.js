import React, { useState, useEffect, useMemo } from 'react';
import Map from '../Map/Map';
import Location from '../Location/Location';
import NextHourGallery from '../Nexthourgallery/Nexthourgallery';
import AQICharts from '../Aqicharts/Aqicharts';
import { LOCATIONS, generateForecast } from '../../utils/Aqihelpers';
import './style.css';

export default function Home() {
  const [activeIndex, setActiveIndex] = useState(0);
  const [liveTime, setLiveTime]       = useState('');

  const activeLocation = LOCATIONS[activeIndex];

  // Regenerate forecast whenever the active location changes
  const forecast = useMemo(
    () => generateForecast(activeLocation.aqi, 12),
    [activeIndex] // eslint-disable-line react-hooks/exhaustive-deps
  );

  // Live clock
  useEffect(() => {
    function tick() {
      setLiveTime(new Date().toLocaleTimeString('en-GB', {
        hour: '2-digit', minute: '2-digit', second: '2-digit',
      }));
    }
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="home">

      {/* ── Header ── */}
      <header className="header">
        <div className="header-left">
          <div className="live-dot" aria-hidden="true" />
          <h1>Air Quality Monitoring — Hanoi</h1>
        </div>
        <span className="live-time" aria-label={`Current time: ${liveTime}`}>
          {liveTime}
        </span>
      </header>

      {/* ── Location selector ── */}
      <nav className="loc-bar" aria-label="Select district">
        {LOCATIONS.map((loc, i) => (
          <button
            key={loc.name}
            className={`loc-btn${i === activeIndex ? ' loc-btn--active' : ''}`}
            onClick={() => setActiveIndex(i)}
            aria-pressed={i === activeIndex}
          >
            {loc.name}
          </button>
        ))}
      </nav>

      {/* ── Map + Info panel ── */}
      <div className="main-grid">
        <Map
          locations={LOCATIONS}
          activeIndex={activeIndex}
          onSelectLocation={setActiveIndex}
        />

        <div className="right-col">
          <Location location={activeLocation} />
          <NextHourGallery forecast={forecast} />
        </div>
      </div>

      {/* ── Charts ── */}
      <AQICharts location={activeLocation} locations={LOCATIONS} />

    </div>
  );
}