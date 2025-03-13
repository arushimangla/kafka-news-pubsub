// Import Kafka from kafkajs
const { Kafka } = require("kafkajs");

// Use an environment variable for the Kafka broker if available, otherwise default to "kafka:9092"
const kafka = new Kafka({
  clientId: "weather-alerts-consumer",
  brokers: [process.env.KAFKA_BROKER || "kafka:9092"],
});

// Create a consumer instance with a specific group ID
const consumer = kafka.consumer({ groupId: "weather-alerts-group" });

// Set the heartbeat interval (for logging/debug purposes)
const HEARTBEAT_INTERVAL = 5000; // 5 seconds

/**
 * consumeWeatherAlerts: Connects to Kafka, subscribes to the specified topic (based on location),
 * and processes incoming messages by calling the provided callback.
 *
 * @param {string} location - The location name to build the topic name.
 * @param {function} callback - The callback function to be invoked for each received message.
 */
const consumeWeatherAlerts = async (location, callback) => {
  try {
    // Connect the consumer
    await consumer.connect();
    console.log(`✅ Connected to Kafka Consumer for ${location}`);

    // Build the topic name (e.g. "California-weather-alerts-topic")
    const topic = `${location}-weather-alerts-topic`;

    // Subscribe to the topic (do not start from the beginning)
    await consumer.subscribe({ topic, fromBeginning: false });
    console.log(`📩 Subscribed to topic: ${topic}`);

    // Start consuming messages using eachBatch to handle messages in batches with proper ordering and heartbeat
    await consumer.run({
      eachBatchAutoResolve: false, // Disable auto-resolving to manually control offset resolution
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        console.log(`🔄 Processing batch of ${batch.messages.length} messages for ${location}`);

        // Process each message in the batch
        for (const message of batch.messages) {
          try {
            const data = JSON.parse(message.value.toString());
            console.log(`🔔 Ordered message received: ${data.eventType}`);

            // Send the data to the provided callback (e.g. to forward it to a WebSocket client)
            callback(data);

            // Mark the message as processed by resolving the offset
            resolveOffset(message.offset);

            // Send a heartbeat to keep the consumer session alive
            await heartbeat();
          } catch (err) {
            console.error("❌ Error processing Kafka message:", err);
          }
        }
      },
    });

    // OPTIONAL: Periodically send a heartbeat for extra assurance (this is not strictly necessary if
    // eachBatch's heartbeat is being called, but can help if your processing is long running)
    setInterval(async () => {
      try {
        await consumer.heartbeat();
        console.log(`💓 Heartbeat sent to Kafka for ${location}`);
      } catch (err) {
        console.error("❌ Error sending periodic heartbeat:", err);
      }
    }, HEARTBEAT_INTERVAL);
  } catch (error) {
    console.error("❌ Error starting Kafka consumer:", error);
  }
};

// Export the consumeWeatherAlerts function for use in other modules (e.g. your WebSocket server)
module.exports = { consumeWeatherAlerts };
