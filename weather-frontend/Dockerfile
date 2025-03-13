# # Use Node.js as the base image
# FROM node:18

# # Set the working directory
# WORKDIR /app

# # Install dependencies required for node-gyp
# RUN apt-get update && apt-get install -y python3 make g++ && \
#     ln -s /usr/bin/python3 /usr/bin/python

FROM node:18 AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm install
COPY . .
RUN npm run build

FROM node:18-alpine
WORKDIR /app
COPY --from=builder /app/build ./build
RUN npm install -g serve
EXPOSE 3000
CMD ["serve", "-s", "build", "-l", "3000"]

# # Copy package files and install dependencies
# COPY package.json package-lock.json ./
# RUN npm install

# # Copy the rest of the application
# COPY . .

# # Expose frontend port
# EXPOSE 3000

# # Start the application
# CMD ["npm", "start"]
