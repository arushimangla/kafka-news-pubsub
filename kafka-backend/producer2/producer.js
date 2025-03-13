const { Kafka } = require("kafkajs");
const fetch = (...args) => import("node-fetch").then(({ default: fetch }) => fetch(...args));


const kafka = new Kafka({
  clientId: "weather-alerts",
  brokers: ["kafka:9092"],
});

const producer = kafka.producer();
const admin = kafka.admin(); // Admin client for leader election
const OPENWEATHER_API_KEY = "5086ab6ea8934e48c2a4767d7da38687";

// Function to fetch the leader of a topic
const getTopicLeader = async (topic) => {
  await admin.connect();
  const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
  await admin.disconnect();

  if (metadata.topics.length > 0) {
    const leaderId = metadata.topics[0].partitions[0].leader;
    console.log(`🏆 Current leader for ${topic} is broker ${leaderId}`);
    return leaderId;
  }

  return null;
};



// Fetch weather alerts from OpenWeather API
const fetchWeatherAlerts = async (lat, lon) => {
  const url = `https://api.openweathermap.org/data/2.5/onecall?lat=${lat}&lon=${lon}&exclude=hourly,daily&appid=${OPENWEATHER_API_KEY}`;
  try {
    const response = await fetch(url);
    const data = await response.json();
    console.log(`🌩️ Weather API response for (${lat}, ${lon}):`, data);

    return data.alerts || [];
  } catch (error) {
    console.error("❌ Error fetching weather alerts:", error);
    return [];
  }
};

// Fetch fire-related disasters from FEMA API (Top 10)
const fetchFireDisasters = async (state) => {
  try {
    console.log(`🔥 Fetching latest fire alerts for ${state}...`);

    const response = await fetch(
      `https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries?state=${state}&incidentType=Fire`
    );
    const data = await response.json();

    if (!data || !data.DisasterDeclarationsSummaries) {
      console.warn(`⚠️ No fire disaster data found for ${state}`);
      return [];
    }

    // Extract fire-related disasters & sort by latest declaration date
    const fireIncidents = data.DisasterDeclarationsSummaries
      .filter((incident) => incident.incidentType === "Fire")
      .sort((a, b) => new Date(b.declarationDate) - new Date(a.declarationDate)) // Latest first
      .slice(0, 10); // Get only top 10

    return fireIncidents.map((incident) => ({
      type: "alert",
      eventType: "Fire",
      description: incident.declarationTitle || "No description available",
      startDate: incident.declarationDate || "N/A",
      url: incident.disasterNumber
        ? `https://www.fema.gov/disaster/${incident.disasterNumber}`
        : "https://www.fema.gov/disaster/",
    }));
  } catch (error) {
    console.error(`❌ Error fetching fire disasters for ${state}:`, error);
    return [];
  }
};

// Function to send alerts to Kafka
const sendToKafka = async (topic, fireAlerts) => {
  try {
    if (fireAlerts.length === 0) {
      console.warn(`⚠️ No fire alerts to send for topic: ${topic}`);
      return;
    }

    // Ensure Kafka leader is available
    const leader = await getTopicLeader(topic);
    if (!leader) {
      console.error("❌ No Kafka leader found. Aborting message production.");
      return;
    }

    await producer.send({
      topic,
      messages: fireAlerts.map((alert) => ({ value: JSON.stringify(alert) })),
    });

    console.log(`✅ Sent ${fireAlerts.length} fire alerts to Kafka topic: ${topic}`);
  } catch (error) {
    console.error("❌ Error sending messages to Kafka:", error);
  }
};

// New function to send a manual alert
const sendWeatherAlert = async (city, alertMessage) => {
  try {
    const topic = `${city}-weather-alerts-topic`;
    await sendToKafka(topic, [{ type: "alert", eventType: "Manual", description: alertMessage, url: "#" }]);
    console.log(`✅ Sent manual alert for ${city}: ${alertMessage}`);
  } catch (error) {
    console.error("❌ Error sending manual weather alert:", error);
  }
};

// Function to fetch and publish fire alerts
const fetchAndPublishFireAlerts = async () => {
  const states = ["California", "Florida", "Washington"];

  for (const state of states) {
    console.log(`🚀 Fetching fire alerts for ${state}`);
    const fireAlerts = await fetchFireDisasters(state);
    const allAlerts = [...fireAlerts];

    await sendToKafka(`${state}-weather-alerts-topic`, allAlerts);
  }
};

// Ensure the Kafka producer connects only once
const startProducer = async () => {
  try {
    await producer.connect();
    console.log("✅ Connected to Kafka producer");

    await fetchAndPublishFireAlerts();
    setInterval(fetchAndPublishFireAlerts, 600000); // Every 10 minutes
  } catch (error) {
    console.error("❌ Error starting producer:", error);
  }
};

startProducer();

module.exports = { sendWeatherAlert };
