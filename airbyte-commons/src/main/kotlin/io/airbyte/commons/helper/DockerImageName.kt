package io.airbyte.commons.helper

object DockerImageName {
  fun extractTag(imageName: String): String {
    return imageName.split(":").last()
  }
}
