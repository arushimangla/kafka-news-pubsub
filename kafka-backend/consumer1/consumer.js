const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "weather-alerts-consumer",
  brokers: ["kafka:9092"],
});

const consumer = kafka.consumer({ groupId: "weather-alerts-group" });

// Heartbeat mechanism
const HEARTBEAT_INTERVAL = 5000; // 5 seconds

const consumeWeatherAlerts = async (location, callback) => {
  try {
    await consumer.connect();
    console.log(`✅ Connected to Kafka Consumer for ${location}`);

    const topic = `${location}-weather-alerts-topic`;

    await consumer.subscribe({ topic, fromBeginning: false });
    console.log(`📩 Subscribed to topic: ${topic}`);

    // Start consuming messages
    await consumer.run({
      eachBatchAutoResolve: false, // Ensure ordering within batch
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        console.log(`🔄 Processing batch of ${batch.messages.length} messages for ${location}`);

        for (const message of batch.messages) {
          try {
            const data = JSON.parse(message.value.toString());
            console.log(`🔔 Ordered message received: ${data.eventType}`);

            // ✅ Send message to WebSocket callback
            callback(data);
            resolveOffset(message.offset); // Mark as processed
            await heartbeat(); // Send heartbeat
          } catch (err) {
            console.error("❌ Error processing Kafka message:", err);
          }
        }
      },
    });

    // Send heartbeats every 5 seconds
    setInterval(async () => {
      await consumer.heartbeat();
      console.log(`💓 Heartbeat sent to Kafka for ${location}`);
    }, HEARTBEAT_INTERVAL);
  } catch (error) {
    console.error("❌ Error starting Kafka consumer:", error);
  }
};

module.exports = { consumeWeatherAlerts };
