# the keycloak version here should be kept in sync with the keycloak version in deps.toml
# Keycloak is hosted on quay, which has historically been unreliable. Increase reliability
# by mirroring the image on dockerhub, which experience has shown to be more reliable.
# This also lets us take advantage of our pull-through cache.
# Thus, when upgrading this image, be sure to also upgrade the mirrored image on dockerhub.
# See README.md for instructions.
FROM airbyte/mirrored-keycloak:26.2

WORKDIR /opt/keycloak

# Blacklist recommended by pen testers: https://github.com/danielmiessler/SecLists/blob/master/Passwords/Common-Credentials/10k-most-common.txt
COPY bin/password-blacklists/10k-most-common.txt data/password-blacklists/10k-most-common.txt
COPY bin/scripts/entrypoint.sh entrypoint.sh
COPY bin/scripts/configure_keycloak.sh configure_keycloak.sh
COPY bin/themes themes

# Doing this instead of creating a separate file and copying it to ensure that we get any keycloak updates to this conf file.
RUN cp conf/cache-ispn.xml conf/cache-ispn-override.xml && \
sed -i conf/cache-ispn-override.xml -e 's/<distributed-cache name=\"sessions\" owners=\"1\">/<distributed-cache name=\"sessions\" owners=\"3\">/g' && \
sed -i conf/cache-ispn-override.xml -e 's/<distributed-cache name=\"authenticationSessions\" owners=\"2\">/<distributed-cache name=\"authenticationSessions\" owners=\"3\">/g' && \
# Make sure that the two lines we wanted to be there are actually there
# i.e. keycloak didn't change its config file
grep '<distributed-cache name="sessions" owners="3">' conf/cache-ispn-override.xml -q && \
grep '<distributed-cache name="authenticationSessions" owners="3">' conf/cache-ispn-override.xml -q && \
# Create the directory for the infinispan global-state persistence
mkdir -p /opt/keycloak/data/infinispan && \
# Inserting the <global-state> block after the <cache-container> start tag
sed -i '/<cache-container /a \    <global-state unclean-shutdown-action="IGNORE"><persistent-location path="\/opt\/keycloak\/data\/infinispan"\/><\/global-state>' conf/cache-ispn-override.xml && \
# Make sure that the <global-state> block is actually there
# i.e. keycloak didn't change its config file
grep '<global-state unclean-shutdown-action="IGNORE"><persistent-location path="/opt/keycloak/data/infinispan"/></global-state>' conf/cache-ispn-override.xml -q

ENTRYPOINT ["./entrypoint.sh"]
