#!/bin/bash
echo "Wiping everything..."
docker compose down -v
rm -f *.log
rm -f config/knoggin.json
rm -rf config/chroma_db
echo "Restarting containers..."
docker compose up -d redis memgraph memgraph-lab --wait
echo "Done."