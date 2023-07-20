# the keycloak version here should be kept in sync with the keycloak version in deps.toml
FROM quay.io/keycloak/keycloak:21.1

WORKDIR /opt/keycloak

COPY bin/scripts/entrypoint.sh entrypoint.sh
COPY bin/themes/airbyte-keycloak-theme themes/airbyte-keycloak-theme

ENTRYPOINT ["./entrypoint.sh"]
