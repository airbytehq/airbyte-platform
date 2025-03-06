# Base Docker Image for Java + Python

This Docker image provides the base for any Java-based Airbyte module that needs to start processes in Docker or Kube.  It is based on the [airbyte/airbyte-base-java-image](../airbyte-base-java-image/) image.

# Releasing

To release a new version of this base image, use the [GitHub Actions workflow](https://github.com/airbytehq/airbyte-platform-internal/actions/workflows/build-base-images.yml).

The workflow will build and publish the images to DockerHub with multi-platform support (linux/amd64, linux/arm64).

To see existing versions, [view the image on Dockerhub](https://hub.docker.com/r/airbyte/airbyte-base-java-worker-image).

After publishing new versions, you may want to update references to these images in the codebase.

[dockerhub]: https://hub.docker.com/repository/docker/airbyte/airbyte-base-java-worker-image/general
