/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.exception

/**
 * Exception thrown when a pod fails to pull container images.
 * This typically indicates issues like:
 * - Image not found in registry
 * - Registry authentication failure
 * - Network issues reaching registry
 * - Invalid image name/tag
 */
class ImagePullException : KubeClientException {
  constructor(
    message: String,
    cause: Throwable,
    commandType: KubeCommandType = KubeCommandType.WAIT_INIT,
    podType: PodType? = null,
  ) : super(
    message = message,
    cause = cause,
    commandType = commandType,
    podType = podType,
  )

  constructor(
    message: String,
    commandType: KubeCommandType = KubeCommandType.WAIT_INIT,
    podType: PodType? = null,
  ) : super(
    message = message,
    cause = RuntimeException(message),
    commandType = commandType,
    podType = podType,
  )
}
