# Password Blacklists

These files are packaged into the Airbyte Keycloak image and referenced by
Keycloak realm password policies.

## `10k-most-common.txt`

Source:
https://github.com/danielmiessler/SecLists/blob/master/Passwords/Common-Credentials/10k-most-common.txt

This was the original password blacklist recommended by penetration testers.
It is still packaged for rollout compatibility while existing realms reference
`passwordBlacklist(10k-most-common.txt)`.

## `common-and-breached-passwords.txt`

Generated from:

- `10k-most-common.txt`
- SecLists `xato-net-10-million-passwords-dup.txt`
- Airbyte-specific context terms

The SecLists README describes `xato-net-10-million-passwords-dup.txt` as the
passwords that appeared more than once in the original xato ten-million
username/password credential dataset.

Source:
https://github.com/danielmiessler/SecLists/blob/master/Passwords/Common-Credentials/xato-net-10-million-passwords-dup.txt

SecLists is MIT licensed:
https://github.com/danielmiessler/SecLists/blob/master/LICENSE
