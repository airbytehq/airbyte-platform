# the keycloak version here should be kept in sync with the keycloak version in deps.toml
# Keycloak is hosted on quay, which has historically been unreliable. Increase reliability
# by mirroring the image on dockerhub, which experience has shown to be more reliable.
# This also lets us take advantage of our pull-through cache.
# Thus, when upgrading this image, be sure to also upgrade the mirrored image on dockerhub.
# See README.md for instructions.
FROM airbyte/mirrored-keycloak:21.1

WORKDIR /opt/keycloak

COPY bin/scripts/entrypoint.sh entrypoint.sh
COPY bin/themes/airbyte-keycloak-theme themes/airbyte-keycloak-theme

ENTRYPOINT ["./entrypoint.sh"]
