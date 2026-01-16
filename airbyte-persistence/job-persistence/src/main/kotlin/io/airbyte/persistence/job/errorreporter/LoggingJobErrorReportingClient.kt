/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter

import io.airbyte.config.FailureReason
import io.airbyte.config.StandardWorkspace
import io.github.oshai.kotlinlogging.KotlinLogging

/**
 * Log job error reports.
 */
class LoggingJobErrorReportingClient : JobErrorReportingClient {
  override fun reportJobFailureReason(
    workspace: StandardWorkspace?,
    reason: FailureReason,
    dockerImage: String?,
    metadata: Map<String?, String?>?,
    attemptConfig: AttemptConfigReportingContext?,
  ) {
    var err =
      mapOf(
        "workspaceId" to workspace?.workspaceId,
        "dockerImage" to dockerImage,
        "metadata" to metadata,
        "state" to attemptConfig?.state,
        "sourceConfig" to attemptConfig?.sourceConfig,
        "destinationConfig" to attemptConfig?.destinationConfig,
      )
    log.info { "Report Job Error -> $err" }
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
