# Dockerfile
FROM node:22-bookworm-slim

# Needed for native addons like sqlite3/better-sqlite3 (if present)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 make g++ \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps first for better caching
COPY package*.json ./
# Use CI in prod builds; in dev you'll mount node_modules as a volume if you like
RUN npm ci

# Copy source
COPY . .

ENV NODE_ENV=production \
    PORT=3000

# Non-root
# RUN useradd -m appuser && chown -R appuser:appuser /app
# USER appuser

EXPOSE 3000
# Adapt if your entry is different (Express generator uses bin/www)
CMD ["npm", "run", "start"]