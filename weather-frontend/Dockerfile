# Use Node.js as the base image
FROM node:18

# Set the working directory
WORKDIR /app

# Install dependencies required for node-gyp
RUN apt-get update && apt-get install -y python3 make g++ && \
    ln -s /usr/bin/python3 /usr/bin/python

# Copy package files and install dependencies
COPY package.json package-lock.json ./
RUN npm install

# Copy the rest of the application
COPY . .

# Expose frontend port
EXPOSE 3000

# Start the application
CMD ["npm", "start"]
