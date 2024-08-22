#!/usr/bin/env bash

export KEYCLOAK_ADMIN=$KEYCLOAK_ADMIN_USER
export KEYCLOAK_ADMIN_PASSWORD=$KEYCLOAK_ADMIN_PASSWORD
export KC_DB=postgres
export KC_DB_URL=$KEYCLOAK_DATABASE_URL
export KC_DB_USERNAME=$KEYCLOAK_DATABASE_USERNAME
export KC_DB_PASSWORD=$KEYCLOAK_DATABASE_PASSWORD
export KC_HTTP_PORT=$KEYCLOAK_PORT

LOG_LEVEL=${LOG_LEVEL:-INFO}

if [ -n "$KEYCLOAK_HOSTNAME_URL" ]; then
  # leave this unset if planning to configure frontendUrl at the realm level.
  export KC_HOSTNAME_URL=$KEYCLOAK_HOSTNAME_URL
fi

if [ -n "$KEYCLOAK_HOSTNAME_ADMIN_URL" ]; then
  # leave this unset to let the admin console url be based on the incoming request.
  export KC_HOSTNAME_ADMIN_URL=$KEYCLOAK_HOSTNAME_ADMIN_URL
fi

bin/kc.sh start \
  --proxy edge \
  --hostname-strict false \
  --log-level=$LOG_LEVEL \
  --cache=ispn \
  --cache-stack=kubernetes \
  --health-enabled=true \
  --http-relative-path /auth \
  --cache-config-file=cache-ispn-override.xml \
  --legacy-observability-interface=true \
  # --spi-theme-static-max-age=-1 \
  # --spi-theme-cache-themes=false \
  # --spi-theme-cache-templates=false
  # Uncomment the --spi-theme options above to disable caching, which is useful for theme development
