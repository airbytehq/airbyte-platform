#!/usr/bin/env bash
#
# This script will wait for keycloak to be up and then disable SSL required on the master realm

while true
do
  if bin/kcadm.sh config credentials --server http://localhost:$KEYCLOAK_PORT/auth --realm master --user $KEYCLOAK_ADMIN_USER --password $KEYCLOAK_ADMIN_PASSWORD
  then
    bin/kcadm.sh update realms/master -s sslRequired=none
    break
  else
    echo "Waiting for Keycloak to start"
    sleep 5
  fi
done
