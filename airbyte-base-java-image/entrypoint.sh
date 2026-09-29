#!/usr/bin/env bash

set -e

# Refresh the system trust store so CA certificates mounted into
# /usr/share/pki/ca-trust-source/anchors or /etc/pki/ca-trust/source/anchors are trusted.
# This must not prevent startup: when the filesystem is read-only the build-time trust store is used.
D=/etc/pki/ca-trust/extracted/pem/directory-hash

# p11-kit can only regenerate directory-hash when the current user owns it. Under an arbitrary UID
# (e.g. OpenShift) it is owned by the airbyte user, so remove it and let update-ca-trust recreate it.
if [ -d "$D" ] && [ ! -O "$D" ] && [ -w "$(dirname "$D")" ]; then
  rm -rf "$D" 2>/dev/null || true
fi

# update-ca-trust always fails to create the /etc/ssl/certs compatibility symlinks when not running
# as root because p11-kit leaves directory-hash read-only; they are recreated below.
update-ca-trust 2>&1 | grep -v "failed to create symbolic link" >&2 || true

if [ -d "$D" ] && chmod u+w "$D" 2>/dev/null; then
  ln -sf ../tls-ca-bundle.pem "$D/ca-certificates.crt"
  ln -sf ../tls-ca-bundle.pem "$D/ca-bundle.crt"
  chmod u-w "$D"
fi

exec "$@"
