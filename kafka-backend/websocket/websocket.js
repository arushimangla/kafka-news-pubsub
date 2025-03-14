const WebSocket = require("ws");
const { Kafka } = require("kafkajs");

// Use environment variable for the Kafka broker if set, otherwise default to "kafka:9092"
const kafka = new Kafka({
  clientId: "websocket-server",
  brokers: [process.env.KAFKA_BROKER || "kafka:9092"],
});

// ------------------------
// Import multiple producer and consumer modules
// ------------------------
// We assume that you have multiple producer and consumer implementations in different folders.
const producers = [
  require("./producer1/producer"),
  require("./producer2/producer"),
  require("./producer3/producer"),
];

const consumers = [
  require("./consumer1/consumer"),
  require("./consumer2/consumer"),
  require("./consumer3/consumer"),
];

const app = express();

// Add a health check endpoint that returns 200 OK.
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Create an HTTP server using the Express app.
const server = http.createServer(app);

// ------------------------
// Create a WebSocket server that attaches to the same HTTP server on port 5000.
// ------------------------
const wss = new WebSocket.Server({ server });

// // Create a new WebSocket server on port 5000
// const wss = new WebSocket.Server({ port: 5000 });

console.log("🚀 WebSocket server is running on ws://localhost:5000/");

// ------------------------
// Leader Election Mechanism
// ------------------------
// This function uses the first producer to send an election message and
// sets up consumers to listen for the election event.
let isLeader = false;
const electionTopic = "leader-election";

async function electLeader() {
  // Connect all producers (if they export a connect method)
  for (const prod of producers) {
    if (typeof prod.connect === "function") {
      await prod.connect();
    }
  }

  // Send an election message using the first producer
  await producers[0].sendMessage(electionTopic, {
    event: "election",
    timestamp: Date.now(),
  });

  // Connect and subscribe all consumers to the election topic
  for (const cons of consumers) {
    if (typeof cons.connect === "function") {
      await cons.connect();
    }
    if (typeof cons.subscribe === "function") {
      await cons.subscribe(electionTopic);
    }
    // Set up message consumption for leader election
    cons.consumeMessages(async (message) => {
      console.log("📩 Election message received:", message);
      if (!isLeader) {
        isLeader = true;
        console.log("⚡ WebSocket server is now the leader!");
      }
    });
  }
}

// ------------------------
// Heartbeat Mechanism
// ------------------------
// If this instance is the leader, send a heartbeat periodically using the first producer.
setInterval(async () => {
  if (isLeader && typeof producers[0].sendMessage === "function") {
    await producers[0].sendMessage("heartbeat", {
      event: "heartbeat",
      timestamp: Date.now(),
    });
    console.log("💓 Heartbeat sent");
  }
}, 5000);

// ------------------------
// WebSocket Connection Handling
// ------------------------
wss.on("connection", (ws) => {
  console.log("🔗 New WebSocket connection");

  // When a client sends a message, forward it to Kafka using a randomly selected producer.
  ws.on("message", async (message) => {
    console.log("📩 Received:", message);
    const selectedProducer = producers[Math.floor(Math.random() * producers.length)];
    if (typeof selectedProducer.sendMessage === "function") {
      await selectedProducer.sendMessage("weather-alerts", message);
      console.log("✅ Message sent to Kafka:", message);
    } else {
      console.error("❌ Selected producer does not support sendMessage");
    }
  });

  // For each consumer, forward any messages received from Kafka to this WebSocket client.
  consumers.forEach((consumer) => {
    consumer.consumeMessages((message) => {
      console.log("📥 Kafka message received:", message);
      ws.send(JSON.stringify({ event: "weather-alert", message }));
    });
  });

  ws.on("close", () => console.log("❌ WebSocket connection closed"));
  ws.on("error", (error) => console.error("WebSocket error:", error));
});

// Start the leader election process
electLeader().catch(console.error);
