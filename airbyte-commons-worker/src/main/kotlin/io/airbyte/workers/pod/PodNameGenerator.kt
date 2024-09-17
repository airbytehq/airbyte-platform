/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.pod

import io.airbyte.workers.pod.PodConstants.KUBE_NAME_LEN_LIMIT
import io.airbyte.workers.pod.PodConstants.REPL_POD_PREFIX
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

@Singleton
class PodNameGenerator(
  @Value("\${airbyte.worker.job.kube.namespace}") val namespace: String,
) {
  fun getReplicationPodName(
    jobId: String,
    attemptId: Long,
  ): String {
    return "$REPL_POD_PREFIX-job-$jobId-attempt-$attemptId"
  }

  fun getCheckPodName(
    image: String,
    jobId: String,
    attemptId: Long,
  ): String {
    return PodUtils.createProcessName(
      image,
      "check",
      jobId,
      attemptId.toInt(),
      KUBE_NAME_LEN_LIMIT,
    )
  }

  fun getDiscoverPodName(
    image: String,
    jobId: String,
    attemptId: Long,
  ): String {
    return PodUtils.createProcessName(
      image,
      "discover",
      jobId,
      attemptId.toInt(),
      KUBE_NAME_LEN_LIMIT,
    )
  }

  fun getSpecPodName(
    image: String,
    jobId: String,
    attemptId: Long,
  ): String {
    return PodUtils.createProcessName(
      image,
      "spec",
      jobId,
      attemptId.toInt(),
      KUBE_NAME_LEN_LIMIT,
    )
  }
}
