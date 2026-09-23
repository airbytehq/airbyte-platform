/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import io.airbyte.commons.logging.DEFAULT_JOB_LOG_PATH_MDC_KEY
import io.airbyte.commons.storage.GcsLogUploadTarget
import io.airbyte.commons.storage.LogUploadAuthorizationRefreshResult
import io.airbyte.commons.storage.RefreshableGcsLogUploader

/** Logback adapter for direct, refreshable GCS job-log delivery. */
class AirbyteFlexLogbackAppender internal constructor(
  private val uploader: RefreshableGcsLogUploader<ILoggingEvent>,
  private val encoder: AirbyteLogEventEncoder,
) : AppenderBase<ILoggingEvent>() {
  constructor(
    initialTarget: GcsLogUploadTarget,
    refreshAuthorization: () -> LogUploadAuthorizationRefreshResult,
    encoder: AirbyteLogEventEncoder = AirbyteLogEventEncoder(),
  ) : this(RefreshableGcsLogUploader(initialTarget, refreshAuthorization, encoder::bulkEncode), encoder)

  override fun start() {
    encoder.start()
    uploader.start()
    super.start()
  }

  override fun stop() {
    uploader.stop()
    encoder.stop()
    super.stop()
  }

  override fun append(eventObject: ILoggingEvent) {
    if (eventObject.level.levelInt < Level.INFO_INT || eventObject.mdcPropertyMap[DEFAULT_JOB_LOG_PATH_MDC_KEY].isNullOrBlank()) {
      return
    }
    uploader.append(eventObject)
  }
}
