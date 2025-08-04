#!/bin/sh

set -euo pipefail

ADMIN_EMAIL=${MB_ADMIN_EMAIL:-admin@example.com}
ADMIN_PASSWORD=${MB_ADMIN_PASSWORD:-admin}

METABASE_HOST=${MB_HOSTNAME:-metabase}
METABASE_PORT=${MB_PORT:-3000}

HAS_USER_SETUP=$(curl -s http://${METABASE_HOST}:${METABASE_PORT}/api/session/properties | jq '.["has-user-setup"]')

if [ "$HAS_USER_SETUP" = "true" ]; then
  echo "Metabase is already initialized. Skipping setup."
  exit 0
fi

echo "Creating Metabase admin user"

SETUP_TOKEN=$(curl -s -X GET \
  -H "Content-Type: application/json" \
  http://${METABASE_HOST}:${METABASE_PORT}/api/session/properties \
  | jq -r '.["setup-token"]')

curl -s -X POST \
  -H "Content-Type: application/json" \
  http://${METABASE_HOST}:${METABASE_PORT}/api/setup \
  -d '{
    "token": "'${SETUP_TOKEN}'",
    "user": {
        "email": "'${ADMIN_EMAIL}'",
        "password": "'${ADMIN_PASSWORD}'"
    },
    "prefs": {
        "allow_tracking": false,
        "site_name": "Metawhat"
    }
}'

echo "Admin user ${ADMIN_EMAIL} created!"