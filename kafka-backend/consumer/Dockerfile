# Use Node.js base image
FROM node:18

# Set working directory
WORKDIR /app

# Copy package files and install dependencies
COPY package.json package-lock.json ./
RUN npm install

# Copy the rest of the application
COPY . .

# Set environment variables for Kafka
ENV KAFKA_BROKER="kafka:9092"
ENV GROUP_ID="weather-alerts-group"

# Expose necessary ports
EXPOSE 9092

# Start the consumer service
CMD ["node", "consumer.js"]
