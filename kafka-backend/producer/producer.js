// Import required modules
const { Kafka } = require("kafkajs");
const fetch = require("node-fetch");

// Allow the Kafka broker to be overridden by an environment variable (useful for Docker)
const kafka = new Kafka({
  clientId: "weather-alerts",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

// Create a Kafka producer and admin client
const producer = kafka.producer();
const admin = kafka.admin(); // Used for leader election and topic metadata
const OPENWEATHER_API_KEY = "5086ab6ea8934e48c2a4767d7da38687";

// Function to fetch the leader of a topic using the admin client
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

// ------------------------
// NEW: Generic sendMessage function
// ------------------------
// This function wraps the producer.send() call and converts messages to JSON strings.
const sendMessage = async (topic, messageObj) => {
  try {
    // Check that there is a leader available for this topic
    const leader = await getTopicLeader(topic);
    if (!leader) {
      console.error("❌ No Kafka leader found. Aborting message production.");
      return;
    }
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(messageObj) }],
    });
    console.log(`✅ Sent message to Kafka topic: ${topic}`);
  } catch (error) {
    console.error("❌ Error sending message to Kafka:", error);
  }
};

// ------------------------
// NEW: sendWeatherAlert helper function
// ------------------------
// This function sends a manual weather alert using the generic sendMessage function.
const sendWeatherAlert = async (city, alertMessage) => {
  try {
    const topic = `${city}-weather-alerts-topic`;
    await sendMessage(topic, {
      type: "alert",
      eventType: "Manual",
      description: alertMessage,
      url: "#",
    });
    console.log(`✅ Sent manual alert for ${city}: ${alertMessage}`);
  } catch (error) {
    console.error("❌ Error sending manual weather alert:", error);
  }
};

// Function to fetch weather alerts from OpenWeather API
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

// Function to fetch fire-related disasters from FEMA API
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

    // Filter and sort the fire incidents, then take the top 10
    const fireIncidents = data.DisasterDeclarationsSummaries
      .filter((incident) => incident.incidentType === "Fire")
      .sort((a, b) => new Date(b.declarationDate) - new Date(a.declarationDate))
      .slice(0, 10);

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

// Function to fetch and publish fire alerts for multiple states
const fetchAndPublishFireAlerts = async () => {
  const states = ["California", "Florida", "Washington"];
  for (const state of states) {
    console.log(`🚀 Fetching fire alerts for ${state}`);
    const fireAlerts = await fetchFireDisasters(state);
    // Use sendMessage to publish all alerts for the state in one go
    await sendMessage(`${state}-weather-alerts-topic`, fireAlerts);
  }
};

// ------------------------
// Updated startup function
// ------------------------
// We export startProducer as the connect method so it can be invoked externally.
const startProducer = async () => {
  try {
    await producer.connect();
    console.log("✅ Connected to Kafka producer");
    // Immediately fetch and publish fire alerts
    await fetchAndPublishFireAlerts();
    // Schedule periodic fetching every 10 minutes
    setInterval(fetchAndPublishFireAlerts, 600000);
  } catch (error) {
    console.error("❌ Error starting producer:", error);
  }
};

// Export our methods so that other modules (like the WebSocket server) can use them.
module.exports = {
  connect: startProducer, // Expose the startup/connect function
  sendMessage,          // Expose generic message sending
  sendWeatherAlert,     // Expose the manual alert function
};
