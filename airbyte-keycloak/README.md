# Upgrading Keycloak Versions

This guide provides instructions on how to pull `quay.io/keycloak/keycloak:25.0.2` image for `amd64` and `arm64` architectures, tag them, push them to your own repository, and create a multi-arch manifest.

We do so so we can mirror the image on the Airbyte repository and use it in our pull-through cache. This is useful for
enhanced reliability - quay.io, Keycloak's official image registry, has had multiple outages in the past. Each time this happens, all build fail.

To publish the updated mirrored Airbyte Keycloak Docker image, use the [update-mirrored-image.sh](update-mirrored-image.sh) bash script
provided in this repository OR use the following manual steps:

## Steps

1. Set upstream version
```bash
export version=26.2
```

2. Push a copy keycloak into our dockerhub repository
```bash
docker buildx imagetools create \
  --tag airbyte/mirrored-keycloak:$version \
  quay.io/keycloak/keycloak:$version
```

3. Verify the result
```bash
docker buildx imagetools inspect airbyte/mirrored-keycloak:$version
```


