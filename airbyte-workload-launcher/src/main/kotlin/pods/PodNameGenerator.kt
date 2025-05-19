/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import io.airbyte.commons.random.randomAlpha
import io.airbyte.workload.launcher.constants.PodConstants.KUBE_NAME_LEN_LIMIT
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

@Singleton
class PodNameGenerator(
  @Value("\${airbyte.worker.job.kube.namespace}") val namespace: String,
) {
  fun getReplicationPodName(
    jobId: String,
    attemptId: Long,
  ): String = "replication-job-$jobId-attempt-$attemptId"

  fun getCheckPodName(
    image: String,
    jobId: String,
    attemptId: Long,
  ): String =
    createProcessName(
      image,
      "check",
      jobId,
      attemptId.toInt(),
    )

  fun getDiscoverPodName(
    image: String,
    jobId: String,
    attemptId: Long,
  ): String =
    createProcessName(
      image,
      "discover",
      jobId,
      attemptId.toInt(),
    )

  fun getSpecPodName(
    image: String,
    jobId: String,
    attemptId: Long,
  ): String =
    createProcessName(
      image,
      "spec",
      jobId,
      attemptId.toInt(),
    )
}

private val ALPHABETIC = "[a-zA-Z]+".toPattern()

/**
 * Docker image names are by convention separated by slashes.
 * The last portion is the image's name. This is followed by a colon and a version number.
 * e.g. airbyte/scheduler:v1 or gcr.io/my-project/image-name:v2.
 *
 * With these two facts, attempt to construct a unique process name with the image name present.
 */
internal fun createProcessName(
  fullImagePath: String,
  jobType: String,
  jobId: String,
  attempt: Int,
): String {
  // Make image name RFC 1123 subdomain compliant
  val imageName = shortImageName(fullImagePath).replace("_", "-").lowercase()
  val suffix = "$jobType-$jobId-$attempt-${randomAlpha(5).lowercase()}"

  val processName =
    "$imageName-$suffix".let {
      if (it.length > KUBE_NAME_LEN_LIMIT) {
        "${imageName.substring(it.length - KUBE_NAME_LEN_LIMIT)}-$suffix"
      } else {
        it
      }
    }

  val m = ALPHABETIC.matcher(processName).also { it.find() }

  return processName.substring(m.start())
}

/**
 * Docker image names are by convention separated by slashes.
 *
 * The last portion is the image's name. This is followed by a colon and a version number.
 * e.g. airbyte/scheduler:v1 or gcr.io/my-project/image-name:v2.
 * Registry name may also include port number, e.g. registry.internal:1234/my-project/image-name:v2
 *
 * @param imagePath the image name with repository and version e.g. gcr.io/my-project/image-name:v2
 * @return the image name without the repo and version, e.g. image-name
 */
internal fun shortImageName(imagePath: String): String = imagePath.substringAfterLast("/").substringBefore(":")
