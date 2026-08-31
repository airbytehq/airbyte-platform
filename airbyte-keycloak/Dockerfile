# the keycloak version here should be kept in sync with the keycloak version in deps.toml
# Keycloak is hosted on quay, which has historically been unreliable. Increase reliability
# by mirroring the image on dockerhub, which experience has shown to be more reliable.
# This also lets us take advantage of our pull-through cache.
# Thus, when upgrading this image, be sure to also upgrade the mirrored image on dockerhub.
# See README.md for instructions.
FROM airbyte/mirrored-keycloak:26.7.2

WORKDIR /opt/keycloak

# Password blacklists. See password-blacklists/README.md for sources.
COPY bin/password-blacklists/10k-most-common.txt data/password-blacklists/10k-most-common.txt
COPY bin/password-blacklists/common-and-breached-passwords.txt data/password-blacklists/common-and-breached-passwords.txt
COPY bin/scripts/entrypoint.sh entrypoint.sh
COPY bin/scripts/configure_keycloak.sh configure_keycloak.sh
COPY bin/themes themes

# Doing this instead of creating a separate file and copying it to ensure that we get any keycloak updates to this conf file.
# Since Keycloak 26.3+, the caches (and their owners counts) are defined programmatically and are no longer set in cache-ispn.xml,
# so only the <global-state> insertion remains here.
RUN cp conf/cache-ispn.xml conf/cache-ispn-override.xml && \
mkdir -p /opt/keycloak/data/infinispan && \
sed -i '/<cache-container /a \    <global-state unclean-shutdown-action="IGNORE"><persistent-location path="\/opt\/keycloak\/data\/infinispan"\/><\/global-state>' conf/cache-ispn-override.xml && \
# Make sure that the <global-state> block is actually there
# i.e. keycloak didn't change its config file
grep '<global-state unclean-shutdown-action="IGNORE"><persistent-location path="/opt/keycloak/data/infinispan"/></global-state>' conf/cache-ispn-override.xml -q

ENTRYPOINT ["./entrypoint.sh"]
