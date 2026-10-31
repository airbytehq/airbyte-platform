FROM amazoncorretto:21

WORKDIR /app

RUN <<EOF
set -e
# Amazon Linux 2023 locks yum (an alias for dnf) to the release the upstream image was built from;
# --releasever=latest moves to the newest release so security fixes published since then are installed.
yum update -y --releasever=latest
yum install -y --releasever=latest tar shadow-utils findutils
yum clean all
rm -rf /var/cache/dnf /var/cache/yum
# Distrust the root that chains to the compromised eMudhra intermediates (CVE-2026-91403).
# Their AAA Certificate Services cross-signs need no block: AL2023 does not trust that root for TLS.
# Remove once Amazon Linux 2023 ships a ca-certificates update that distrusts it.
d=/etc/pki/ca-trust/source/blocklist; [ -d $d ] || d=/etc/pki/ca-trust/source/blacklist
trust extract --overwrite --format=pem-bundle --filter='pkcs11:object=emSign%20ECC%20Root%20CA%20-%20G3;type=cert' $d/emSign_ECC_Root_CA_-_G3.pem
update-ca-trust
groupadd --gid 1000 airbyte
useradd --uid 1000 --gid airbyte --shell /bin/bash --create-home airbyte
# Create mount point for secrets
mkdir /secrets
mkdir -p /tmp/workspace
mkdir -p /tmp/airbyte_local
# These are the directories the entrypoint or the application writes to. The trust store under
# /etc/pki/ca-trust is included because the entrypoint regenerates it at startup to pick up CA
# certificates customers mount into /usr/share/pki/ca-trust-source/anchors.
#
# Normally the container runs as the airbyte user (UID 1000), which owns these files and can
# write to them. OpenShift instead runs the container as an arbitrary UID that is not the owner
# but is always a member of group 0. For that UID to write here, two things must be true:
#   1. group 0 must be the files' group      -> chown airbyte:0  (owner stays airbyte)
#   2. the group must be allowed to write    -> chmod g=u        (give the group the owner's permissions)
# u+w is needed first because some trust store files ship read-only even for the owner, and
# g=u can only copy a write bit that exists.
for dir in /app /secrets /tmp/workspace /tmp/airbyte_local /home/airbyte /usr/share/pki/ca-trust-source /etc/pki/ca-trust; do
  chown -R airbyte:0 "$dir"
  chmod -R u+w,g=u "$dir"
done
EOF

# Add the Datadog Java APM agent. ADD from a URL defaults to mode 0600, which an arbitrary UID cannot read.
ADD --chown=airbyte:0 --chmod=644 https://dtdg.co/latest-java-tracer dd-java-agent.jar

ADD --chown=airbyte:0 entrypoint.sh /entrypoint.sh

ENV HOME=/home/airbyte

USER airbyte:airbyte

ENTRYPOINT ["/entrypoint.sh"]
