import React,{useEffect,useState,useRef} from "react";
import L from 'leaflet';
import "leaflet/dist/leaflet.css";
import './style.css';
const LEGEND = [
  { label: "Tot",           color: '#00E400' },
  { label:"Trung binh",     color: '#FFFF00' },
  { label:"Kem",   color: '#FF7E00' },
  { label: "Xau",   color: '#FF0000' },
  { label: "Rat xau",color:'#8F3F97'},
    {label:"Nguy hai", color: '#7E0023' },
];
function makeMarkerIcon(aqi,color, isActive) {
  const size  = isActive ? 44 : 36;
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 44 44">
      <circle cx="22" cy="22" r="20" fill="${color}" fill-opacity="0.18" stroke="${color}" stroke-opacity="0.35" stroke-width="1"/>
      <circle cx="22" cy="22" r="${isActive ? 15 : 12}" fill="${color}" fill-opacity="0.9"
        ${isActive ? 'stroke="#ffffff" stroke-width="2.5"' : ''}/>
      <text x="22" y="27" text-anchor="middle" fill="#fff"
        font-size="${aqi >= 100 ? 9 : 10}" font-family="Space Mono,monospace" font-weight="700">${aqi}</text>
    </svg>
  `;
  return L.divIcon({
    html: svg,
    className: '',
    iconSize:   [size, size],
    iconAnchor: [size / 2, size / 2],
    popupAnchor:[0, -(size / 2) - 4],
  });
}
export default function MapV2({ locations, activeIndex, onSelectLocation }){
    const mapContainerRef=useRef(null);
    const map=useRef(null);
    const [lng]=useState(-97.7341);
    const [lat]=useState(30.2672);
    const [zoom]=useState(2);
    const svgRef = useRef(null);
    const markersRef= useRef([]);
    useEffect(()=>{
        map.current=L.map(mapContainerRef.current).setView([lat,lng],zoom);
        const aqiMarkerOptions={
            
        }
        L.tileLayer('http://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}',{
            subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
            attribution:"&copy; <a href='https://www.google.com/maps'>Google Maps</a> contributors",
        }).addTo(map.current);
        return ()=>map.current.remove();
    },[lat,lng,zoom]);
    useEffect(() => {
        const map_ = map.current;
        if (!map_) return;
        // Remove old markers
        markersRef.current.forEach(m => m.remove());
        markersRef.current = [];
    
        locations.forEach((loc, i) => {
          const isActive = i === activeIndex;
          const icon = makeMarkerIcon(loc.aqi_final,loc.color, isActive);
    
          const marker = L.marker([loc.lat, loc.lon], { icon, zIndexOffset: isActive ? 1000 : 0 })
            .addTo(map_);
    
          // Popup
          const popupContent = `
            <div class="lf-popup">
              <div class="lf-popup-name">${loc.district+" "+loc.city}</div>
              <div class="lf-popup-row">
                <span class="lf-popup-label">AQI</span>
                <span class="lf-popup-val" style="color:${loc.color}">${loc.aqi_final} — ${loc.label}</span>
              </div>
              <div class="lf-popup-row">
                <span class="lf-popup-label">PM2.5</span>
                <span class="lf-popup-val">${loc.pm_25} µg/m³</span>
              </div>
              <div class="lf-popup-row">
                <span class="lf-popup-label">Cluster</span>
                <span class="lf-popup-val">${0}</span>
              </div>
            </div>
          `;
    
          marker.bindPopup(popupContent, {
            className: 'lf-popup-wrapper',
            maxWidth: 220,
            closeButton: true,
          });
    
          marker.on('click', () => {
            onSelectLocation(i);
          });
    
          if (isActive) {
            marker.openPopup();
            map_.panTo([loc.lat, loc.lon], { animate: true, duration: 0.4 });
          }
    
          markersRef.current.push(marker);
        });
      }, [locations, activeIndex, onSelectLocation]);
    return(

       
       <div className="map-panel">
      <span className="map-label">Hanoi Districts</span>
      <div ref={mapContainerRef} className="map-leaflet" />

      {/* Legend overlay */}
      <div className="map-legend">
        {LEGEND.map(item => (
          <div key={item.label} className="legend-item">
            <div className="legend-dot" style={{ background: item.color }} />
            <span>{item.label}</span>
          </div>
        ))}
      </div>
    </div>

    )
}
