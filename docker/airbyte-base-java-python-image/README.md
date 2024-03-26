# Base Docker Image for Java + Python

This Docker image provides the base for any Java-based Airbyte module that also needs to run python code.  It is based on the [airbyte/airbyte-base-java-image](../airbyte-base-java-image/) image.

# Releasing

To release a new version of this base image, use the following steps:

1. Log in to [Dockerhub](https://hub.docker.com/) via the Docker CLI (`docker login`).
2. Run `docker buildx create --use` to enable Docker `buildx` if you have not used it previously.
3. Run the following to build and push a new version of this image (replace `<new_version>` with a new version!) :
   ```
   docker buildx build --push \
     --tag airbyte/airbyte-base-java-python-image:<new_version> \
     --platform linux/amd64,linux/arm64 .
   ```
   To see existing versions, [view the image on Dockerhub](https://hub.docker.com/r/airbyte/airbyte-base-java-python-image).
4. Update base Docker image tag to the new version in all Dockerfiles that depend on the base image:
   ```bash
   FROM airbyte/airbyte-base-java-python-image:<NEW VERSION>
   ```

[dockerhub]: https://hub.docker.com/repository/docker/airbyte/airbyte-base-java-python-image/general
