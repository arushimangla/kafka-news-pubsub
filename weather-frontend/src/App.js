import React from 'react';
import WeatherAlerts from './components/WeatherAlerts';
import './App.css';

function App() {
  return (
    <div className="app-container">
      <h1 className="title">Disaster Alert Subscription</h1>
      <p className="description">
        Subscribe to Disaster alerts for your location and get real-time notifications.
      </p>
      <WeatherAlerts />
    </div>
  );
}

export default App;
