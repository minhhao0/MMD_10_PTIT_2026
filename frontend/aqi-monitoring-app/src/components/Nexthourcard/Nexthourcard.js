import React from 'react';
import { aqiColor, aqiCategory } from '../../utils/Aqihelpers';
import './style.css';

/**
 * NextHourCard
 * Props:
 *   hour    {number} — forecast offset in hours (e.g. 1 = "+1h")
 *   aqi     {number} — predicted AQI value
 *   active  {boolean} — whether this card is currently selected
 *   onClick {function} — called when the card is clicked
 */
export default function NextHourCard({ hour, aqi, active, onClick }) {
  const color = aqiColor(aqi);
  const category = aqiCategory(aqi);

  return (
    <div
      className={`fc-card${active ? ' fc-card--active' : ''}`}
      style={active ? { borderColor: color } : {}}
      onClick={onClick}
      role="button"
      tabIndex={0}
      onKeyDown={e => e.key === 'Enter' && onClick()}
      aria-pressed={active}
      aria-label={`+${hour} hour forecast, AQI ${aqi}, ${category}`}
    >
      <div className="fc-time">+{hour}h</div>
      <div className="fc-aqi" style={{ color }}>{aqi}</div>
      <div className="fc-bar" style={{ background: color }} />
      <div className="fc-label" style={{ color }}>{category}</div>
    </div>
  );
}