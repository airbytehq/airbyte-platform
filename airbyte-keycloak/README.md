# Upgrading Keycloak Versions

This guide provides instructions on how to pull `quay.io/keycloak/keycloak:25.0.2` image for `amd64` and `arm64` architectures, tag them, push them to your own repository, and create a multi-arch manifest.

We do so so we can mirror the image on the Airbyte repository and use it in our pull-through cache. This is useful for
enhanced reliability - quay.io, Keycloak's official image registry, has had multiple outages in the past. Each time this happens, all build fail.

To publish the updated mirrored Airbyte Keycloak Docker image, use the [update-mirrored-image.sh](update-mirrored-image.sh) bash script
provided in this repository OR use the following manual steps:

## Steps

1. Pull, tag and push the `amd64` image. Change the version accordingly.
   ```bash
   docker pull --platform linux/amd64 quay.io/keycloak/keycloak:25.0.2
   docker tag quay.io/keycloak/keycloak:25.0.2 airbyte/mirrored-keycloak-amd:25.0.2
   docker push airbyte/mirrored-keycloak-amd:25.0.2
    ```

2. Pull, tag and push the `arm64` image. Change the version accordingly.
   ```bash
   docker pull --platform linux/arm64 quay.io/keycloak/keycloak:25.0.2
   docker tag quay.io/keycloak/keycloak:25.0.2 airbyte/mirrored-keycloak-arm:25.0.2
   docker push airbyte/mirrored-keycloak-arm:25.0.2
    ```

3. Create a multi-arch manifest. This will create a new image `airbyte/mirrored-keycloak:25.0.2` that will be a manifest of the two images above.
    ```bash
    docker manifest create airbyte/mirrored-keycloak:25.0.2
      --amend airbyte/mirrored-keycloak-amd:25.0.2
      --amend airbyte/mirrored-keycloak-arm:25.0.2
    ```

4. Annotate the manifest with the two architectures. This tells Docker which image is associated with what architecture.
   ```bash
   docker manifest annotate airbyte/mirrored-keycloak:25.0.2 airbyte/mirrored-keycloak-amd:25.0.2 --arch amd64
   docker manifest annotate airbyte/mirrored-keycloak:25.0.2 airbyte/mirrored-keycloak-arm:25.0.2 --arch arm64

5. Finally, push the manifest to the repository. You are done!
    ```bash
    docker manifest push airbyte/mirrored-keycloak:25.0.2
    ```
