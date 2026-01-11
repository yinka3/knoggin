#!/bin/bash
# Generate .env with random passwords (run once before docker compose up)

if [ -f .env ]; then
    echo ".env already exists. Delete it to regenerate."
    exit 0
fi

REDIS_PW=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 32)
MG_PW=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 32)

cat > .env << EOF
REDIS_PASSWORD=${REDIS_PW}
MEMGRAPH_USER=vestige
MEMGRAPH_PASSWORD=${MG_PW}
MEMGRAPH_HOST=memgraph
MEMGRAPH_PORT=7687
REDIS_HOST=redis
REDIS_PORT=6379
EOF

echo "Generated .env with secure passwords"