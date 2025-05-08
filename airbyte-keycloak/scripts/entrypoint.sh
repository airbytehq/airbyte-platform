#!/usr/bin/env bash

export KEYCLOAK_ADMIN=$KEYCLOAK_ADMIN_USER
export KEYCLOAK_ADMIN_PASSWORD=$KEYCLOAK_ADMIN_PASSWORD
export KC_DB=postgres
export KC_DB_URL=$KEYCLOAK_DATABASE_URL
export KC_DB_USERNAME=$KEYCLOAK_DATABASE_USERNAME
export KC_DB_PASSWORD=$KEYCLOAK_DATABASE_PASSWORD
export KC_HTTP_ENABLED=true
export KC_HTTP_PORT=$KEYCLOAK_PORT

LOG_LEVEL=${LOG_LEVEL:-INFO}

# Build the command dynamically
CMD="bin/kc.sh start \
  --proxy-headers xforwarded \
  --log-level=$LOG_LEVEL \
  --cache=ispn \
  --cache-stack=kubernetes \
  --health-enabled=true \
  --http-relative-path /auth \
  --cache-config-file=cache-ispn-override.xml \
  --legacy-observability-interface=true \
  --http-enabled=true"
  # --spi-theme-static-max-age=-1 \
  # --spi-theme-cache-themes=false \
  # --spi-theme-cache-templates=false
  # Uncomment the --spi-theme options above to disable caching, which is useful for theme development

# If the KEYCLOAK_HOSTNAME_URL is set, use it as the hostname, and set backchannel to dynamic
# for requests from internal services to Keycloak.
# Otherwise, disable strict hostname checking.
if [ -n "$KEYCLOAK_HOSTNAME_URL" ]; then
  CMD+=" --hostname $KEYCLOAK_HOSTNAME_URL"
  CMD+=" --hostname-backchannel-dynamic true"
else
  CMD+=" --hostname-strict false"
fi

# If the KEYCLOAK_ADMIN_HOSTNAME_URL is set, use it as the admin hostname
if [ -n "$KEYCLOAK_ADMIN_HOSTNAME_URL" ]; then
  CMD+=" --hostname-admin $KEYCLOAK_ADMIN_HOSTNAME_URL"
fi

./configure_keycloak.sh > /dev/null 2>&1 & disown
# Execute the command
eval $CMD
