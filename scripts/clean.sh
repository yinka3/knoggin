#!/bin/bash
echo "Wiping everything..."
docker compose down -v
rm -f *.log
rm -rf config/knoggin.json
echo "Restarting containers..."
docker compose up -d redis memgraph memgraph-lab --wait
echo "Done."