#!/usr/bin/env sh
set -eu

echo "Stopping order-service to verify frontend isolation..."
docker compose stop order-service
docker compose exec -T frontend wget -q -O - http://127.0.0.1/health

echo "Starting order-service again..."
docker compose start order-service

attempt=0
until docker compose exec -T frontend wget -q -O - http://127.0.0.1/api/order/ready >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "order-service did not recover within 60 attempts" >&2
    exit 1
  fi
  sleep 1
done

docker compose exec -T frontend wget -q -O - http://127.0.0.1/api/order/ready
echo
echo "Recovery smoke passed."
