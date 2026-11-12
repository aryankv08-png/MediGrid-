# ----------------------------------------------------------
# Builder stage – compile TypeScript
# ----------------------------------------------------------
FROM node:18-alpine AS builder
WORKDIR /app

# Install production + dev dependencies
COPY package.json ./
RUN npm install

# Copy source files and compile
COPY tsconfig.json ./
COPY public ./public
COPY src ./src
RUN npx tsc --build

# ----------------------------------------------------------
# Runtime stage – run compiled JavaScript
# ----------------------------------------------------------
FROM node:18-alpine
WORKDIR /app

# Copy compiled output and production node_modules from builder
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/public ./public
COPY --from=builder /app/node_modules ./node_modules

ENV NODE_ENV=production
EXPOSE 3000
CMD ["node","dist/index.js"]

