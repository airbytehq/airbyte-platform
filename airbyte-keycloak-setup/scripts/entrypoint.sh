#/bin/bash
until curl --output /dev/null --head --fail http://${KEYCLOAK_INTERNAL_HOST}/auth/health/ready; do sleep 1; done;
/app/airbyte-app/bin/airbyte-keycloak-setup