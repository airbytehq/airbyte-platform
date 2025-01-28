/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.helper

object DockerImageName {
  fun extractTag(imageName: String): String = imageName.split(":").last()
}
