# Use Node.js base image
FROM node:18

# Set working directory
WORKDIR /app

# Copy package files and install dependencies
COPY package.json package-lock.json ./
RUN npm install

# Copy the rest of the application
COPY . .

# Set environment variables
ENV WEBSOCKET_PORT=5000

# Expose WebSocket port
EXPOSE 5000

# Start the WebSocket server
CMD ["node", "websocket.js"]
