import React, { useState, useEffect, useRef } from 'react';
import './WeatherAlerts.css';

const WeatherAlerts = () => {
  const [subscribedTopics, setSubscribedTopics] = useState([]);
  const [alert, setAlert] = useState('');
  const [fireNews, setFireNews] = useState([]);
  const topics = ['California', 'Florida', 'Washington']; // Available topics (States)
  const socketRef = useRef(null); // Store the WebSocket reference
  const topicsRef = useRef(new Set()); // Store active subscriptions

  useEffect(() => {
    // Use wss:// because the ALB likely terminates SSL.
    const wsUrl = "wss://my-weather-app-alb-1-605109522.us-west-2.elb.amazonaws.com:5000";
    
    // Create a single WebSocket connection and store it in the ref.
    socketRef.current = new WebSocket(wsUrl);

    // When the connection opens, log and resubscribe to any topics.
    socketRef.current.onopen = () => {
      console.log('✅ Connected to WebSocket server');
      topicsRef.current.forEach((topic) => {
        socketRef.current.send(JSON.stringify({ subscribe: true, location: topic }));
      });
    };

    // Handle incoming messages.
    socketRef.current.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        console.log("📩 Received data:", data);
    
        // Ensure the received data contains the expected fields.
        if (data.location && data.message) {
          setFireNews((prevNews) => [
            ...prevNews,
            {
              title: data.message.eventType || "Breaking News",
              description: data.message.description || "No description available.",
              startDate: data.message.startDate || "Unknown Date",
              url: data.message.url || "#",
              location: data.location,
            },
          ]);
        } else {
          console.warn("⚠️ Received data but missing required fields:", data);
        }
      } catch (error) {
        console.error("❌ Error parsing WebSocket message:", error);
      }
    };

    // Log any WebSocket errors.
    socketRef.current.onerror = (error) => console.error('⚠️ WebSocket Error:', error);

    // If the connection closes, attempt to reconnect after 5 seconds.
    socketRef.current.onclose = () => {
      console.log('❌ WebSocket connection closed. Reconnecting in 5s...');
      setTimeout(() => {
        if (socketRef.current.readyState === WebSocket.CLOSED) {
          socketRef.current = new WebSocket(wsUrl);
        }
      }, 5000);
    };

    // Clean up the WebSocket connection on component unmount.
    return () => {
      if (socketRef.current) {
        socketRef.current.close();
      }
    };
  }, []);

  // Function to subscribe to a topic.
  const subscribeToTopic = (topic) => {
    if (!topicsRef.current.has(topic)) {
      topicsRef.current.add(topic);
      setSubscribedTopics((prev) => [...prev, topic]);
      console.log(`🔔 Subscribed to ${topic} alerts`);

      // Send subscription message if the WebSocket connection is open.
      if (socketRef.current && socketRef.current.readyState === WebSocket.OPEN) {
        socketRef.current.send(JSON.stringify({ subscribe: true, location: topic }));
      } else {
        console.warn("⚠️ WebSocket not open. Subscription request failed.");
      }
    } else {
      console.log(`ℹ️ Already subscribed to ${topic} alerts`);
    }
  };

  return (
    <div className="weather-alerts">
      <h1>Disaster News</h1>
      <div className="subscription">
        <h2>Subscribe to location</h2>
        {topics.map((topic) => (
          <button key={topic} onClick={() => subscribeToTopic(topic)} className="subscribe-btn">
            {topic} Alerts
          </button>
        ))}
      </div>

      <div className="subscribed-topics">
        <h3>Subscribed Topics:</h3>
        <ul>
          {subscribedTopics.map((topic, index) => (
            <li key={index}>{topic}</li>
          ))}
        </ul>
      </div>

      {alert && <div className="alert"><strong>🚨 {alert}</strong></div>}

      <div className="fire-news">
        <h3>🔥 Latest News</h3>
        {fireNews.length > 0 ? (
          fireNews.map((article, index) => (
            <div key={index} className="news-article">
              <h4>{article.eventType || "Fire Alert"}</h4>
              <p><strong>Date:</strong> {article.startDate}</p>
              <p>{article.description}</p>
              {article.url && article.url !== "#" ? (
                <a 
                  href={article.url.startsWith("http") ? article.url : `https://www.fema.gov/disaster/${article.url}`}
                  target="_blank" 
                  rel="noopener noreferrer"
                >
                  Read more
                </a>
              ) : (
                <p>🔗 No URL available</p>
              )}
            </div>
          ))
        ) : (
          <p>📭 No news available for this location.</p>
        )}
      </div>
    </div>
  );
};

export default WeatherAlerts;
