#!/usr/bin/env bash

set -e

D=/etc/pki/ca-trust/extracted/pem/directory-hash
update-ca-trust || true
chmod u+w "$D"
ln -sf ../tls-ca-bundle.pem "$D/ca-certificates.crt"
ln -sf ../tls-ca-bundle.pem "$D/ca-bundle.crt"
chmod u-w "$D"

exec "$@"
