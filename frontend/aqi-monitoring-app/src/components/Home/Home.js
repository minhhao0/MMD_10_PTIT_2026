import React, { useState, useEffect, useMemo } from 'react';
import Location from '../Location/Location';
import NextHourGallery from '../Nexthourgallery/Nexthourgallery';
import AQICharts from '../Aqicharts/Aqicharts';
import { LOCATIONS, generateForecast } from '../../utils/Aqihelpers';
import './style.css';
import MapV2 from '../Map/Mapv2';


export default function Home() {
  const [activeIndex, setActiveIndex] = useState(0);
  const [liveTime, setLiveTime]       = useState('');
  const [location,setLocation]=useState([]);
  // Regenerate forecast whenever the active location changes
  // const forecast = useMemo(
  //   () => generateForecast(location[activeIndex].aqi_final, 12),
  //   [activeIndex] // eslint-disable-line react-hooks/exhaustive-deps
  // );
  // Live clock
  useEffect(() => {
    function tick() {
      setLiveTime(new Date().toLocaleTimeString('en-GB', {
        hour: '2-digit', minute: '2-digit', second: '2-digit',
      }));
    }
     const fetchLocationData = async () => {
      const response = await fetch(
        "http://127.0.0.1:8000/location"
      );
        const data = await response.json();
        if (response.ok){
            setLocation(data);
        } 
    };
    fetchLocationData();
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
          <h1>Air Quality Monitoring — VietNam</h1>
        </div>
        <span className="live-time" aria-label={`Current time: ${liveTime}`}>
          {liveTime}
        </span>
      </header>

      {/* ── Location selector ── */}
      {/* <nav className="loc-bar" aria-label="Select district">
        { location && location.map((loc, i) => (
          <button
            key={loc.district}
            className={`loc-btn${i === activeIndex ? ' loc-btn--active' : ''}`}
            onClick={() => setActiveIndex(i)}
            aria-pressed={i === activeIndex}
          >
            {loc.district}
          </button>
        ))}
      </nav> */}

      {/* ── Map + Info panel ── */}
      {location && <div className="main-grid">
        {/* <Map
          locations={LOCATIONS}
          activeIndex={activeIndex}
          onSelectLocation={setActiveIndex}
        /> */}
        <MapV2
          locations={location}
          activeIndex={activeIndex}
          onSelectLocation={setActiveIndex}
        />
        <div className="right-col">
          <Location location={location[activeIndex]} />
          {/* <NextHourGallery forecast={forecast} /> */}
        </div>
      </div>}

      {/* ── Charts ── */}
      {location && <AQICharts location={location[activeIndex]} locations={location} />}

    </div>
  );
}
