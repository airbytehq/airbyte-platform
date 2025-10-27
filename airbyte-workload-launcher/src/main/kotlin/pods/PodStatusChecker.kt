/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import io.fabric8.kubernetes.api.model.Pod

/**
 * Utility for checking pod status and detecting common issues like image pull failures.
 */
object PodStatusChecker {
  data class ImagePullError(
    val containerName: String,
    val containerType: ContainerType,
    val image: String?,
    val reason: String,
    val message: String?,
  )

  enum class ContainerType {
    INIT,
    MAIN,
  }

  /**
   * Checks all containers (init and main) in a pod for image pull errors.
   *
   * @param pod The pod to check
   * @return List of image pull errors found, empty if none
   */
  fun checkForImagePullErrors(pod: Pod?): List<ImagePullError> {
    if (pod?.status == null) return emptyList()

    val errors = mutableListOf<ImagePullError>()

    // Check init containers
    pod.status.initContainerStatuses?.forEach { containerStatus ->
      containerStatus.state?.waiting?.let { waiting ->
        if (isImagePullError(waiting.reason)) {
          errors.add(
            ImagePullError(
              containerName = containerStatus.name,
              containerType = ContainerType.INIT,
              image = containerStatus.image,
              reason = waiting.reason,
              message = waiting.message,
            ),
          )
        }
      }
    }

    // Check main containers
    pod.status.containerStatuses?.forEach { containerStatus ->
      containerStatus.state?.waiting?.let { waiting ->
        if (isImagePullError(waiting.reason)) {
          errors.add(
            ImagePullError(
              containerName = containerStatus.name,
              containerType = ContainerType.MAIN,
              image = containerStatus.image,
              reason = waiting.reason,
              message = waiting.message,
            ),
          )
        }
      }
    }

    return errors
  }

  /**
   * Checks if a waiting reason indicates an image pull error.
   * Only catches ImagePullBackOff errors (when Kubernetes is backing off after repeated failures).
   * Does not catch ErrImagePull (initial image pull failure) to avoid premature failures.
   */
  private fun isImagePullError(reason: String?): Boolean = reason == "ImagePullBackOff"

  /**
   * Formats a list of image pull errors into a human-readable string.
   * Creates a concise, user-friendly message focused on the image name.
   */
  fun formatImagePullErrors(errors: List<ImagePullError>): String {
    // For user-facing messages, keep it simple and focused on the image
    return errors.joinToString("; ") { error ->
      if (error.image != null) {
        "Failed to pull container image=${error.image}"
      } else {
        "Failed to pull container image for ${error.containerName}"
      }
    }
  }
}
