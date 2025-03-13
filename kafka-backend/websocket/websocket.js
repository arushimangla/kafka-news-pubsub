const WebSocket = require("ws");
const { Kafka } = require("kafkajs");

// ✅ Connect to Kafka inside Docker
const kafka = new Kafka({
  clientId: "websocket-server",
  brokers: [process.env.KAFKA_BROKER || "kafka:9092"],
});

// ✅ Initialize Three Separate Producers
const producers = [
  require("./producer1/producer"),
  require("./producer2/producer"),
  require("./producer3/producer"),
];

// ✅ Initialize Three Separate Consumers
const consumers = [
  require("./consumer1/consumer"),
  require("./consumer2/consumer"),
  require("./consumer3/consumer"),
];

const wss = new WebSocket.Server({ port: 5000 });

console.log("🚀 WebSocket server is running on ws://localhost:5000/");

// ✅ Leader Election Mechanism
let isLeader = false;
const electionTopic = "leader-election";

async function electLeader() {
  for (const producer of producers) {
    await producer.connect();
  }

  await producers[0].sendMessage("leader-election", {
    event: "election",
    timestamp: Date.now(),
  });

  for (const consumer of consumers) {
    await consumer.connect();
    await consumer.subscribe("leader-election");

    consumer.consumeMessages(async (message) => {
      console.log("📩 Election message received:", message);

      if (!isLeader) {
        isLeader = true;
        console.log("⚡ WebSocket server is now the leader!");
      }
    });
  }
}

// ✅ Heartbeat Mechanism
setInterval(async () => {
  if (isLeader) {
    await producers[0].sendMessage("heartbeat", {
      event: "heartbeat",
      timestamp: Date.now(),
    });
    console.log("💓 Heartbeat sent");
  }
}, 5000);

// ✅ Handle WebSocket Connections
wss.on("connection", async (ws) => {
  console.log("🔗 New WebSocket connection");

  ws.on("message", async (message) => {
    console.log("📩 Received:", message);

    // ✅ Round-robin selection of producers
    const selectedProducer = producers[Math.floor(Math.random() * producers.length)];
    await selectedProducer.sendMessage("weather-alerts", message);

    console.log("✅ Message sent to Kafka:", message);
  });

  // ✅ Consumers read messages from Kafka and send them to WebSocket clients
  consumers.forEach((consumer) => {
    consumer.consumeMessages((message) => {
      console.log("📥 Kafka message received:", message);
      ws.send(JSON.stringify({ event: "weather-alert", message }));
    });
  });

  ws.on("close", () => console.log("❌ WebSocket connection closed"));
});

// ✅ Start leader election
electLeader().catch(console.error);
