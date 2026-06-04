#!/bin/bash
# Generate .env with random passwords (run once before docker compose up)

if [ -f .env ]; then
    echo ".env already exists. Delete it to regenerate."
    exit 0
fi

REDIS_PW=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 32)
PG_PW=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 32)

cat > .env << EOF
REDIS_PASSWORD=${REDIS_PW}
REDIS_HOST=redis
REDIS_PORT=6379
POSTGRES_USER=knoggin
POSTGRES_PASSWORD=${PG_PW}
POSTGRES_DB=knoggin_db
DATABASE_URL=postgresql://knoggin:${PG_PW}@postgres:5432/knoggin_db
EOF

echo "Generated .env with secure passwords"