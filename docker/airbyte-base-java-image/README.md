# Base Docker Image for Java

This Docker image provides the base for any Java-based Airbyte module.  It is currently based on the [Amazon Corretto](https://aws.amazon.com/corretto/?filtered-posts.sort-by=item.additionalFields.createdDate&filtered-posts.sort-order=desc)
distribution of [OpenJDK](https://openjdk.org/).

# Releasing

To release a new version of this base image, use the [GitHub Actions workflow](https://github.com/airbytehq/airbyte-platform-internal/actions/workflows/build-base-images.yml).

The workflow will build and publish the images to DockerHub with multi-platform support (linux/amd64, linux/arm64).

To see existing versions, [view the image on Dockerhub](https://hub.docker.com/r/airbyte/airbyte-base-java-image).

After publishing new versions, you may want to update references to these images in the codebase.

[dockerhub]: https://hub.docker.com/repository/docker/airbyte/airbyte-base-java-image/general
