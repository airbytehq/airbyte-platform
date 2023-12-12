#!/usr/bin/env bash

export KEYCLOAK_ADMIN=$KEYCLOAK_ADMIN_USER
export KEYCLOAK_ADMIN_PASSWORD=$KEYCLOAK_ADMIN_PASSWORD
export KC_DB=postgres
export KC_DB_URL=$KEYCLOAK_DATABASE_URL
export KC_DB_USERNAME=$KEYCLOAK_DATABASE_USERNAME
export KC_DB_PASSWORD=$KEYCLOAK_DATABASE_PASSWORD
export KC_HOSTNAME_URL=$KEYCLOAK_HOSTNAME_URL
export KC_HTTP_PORT=$KEYCLOAK_PORT
export KC_HOSTNAME_ADMIN_URL=$KEYCLOAK_HOSTNAME_ADMIN_URL

bin/kc.sh build --cache=ispn --cache-stack=kubernetes --health-enabled=true --http-relative-path /auth

bin/kc.sh start --optimized --proxy edge --hostname-strict false

# Uncomment to disable caching, which is useful for theme development
# bin/kc.sh start --optimized --proxy edge --hostname-strict false --spi-theme-static-max-age=-1 --spi-theme-cache-themes=false --spi-theme-cache-templates=false