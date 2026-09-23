/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.fasterxml.jackson.core.JsonProcessingException
import io.airbyte.api.client.ApiException
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.logging.logback.AirbyteFlexLogbackAppender
import io.airbyte.commons.logging.logback.CLOUD_OPERATIONS_JOB_LOGGER_NAME
import io.airbyte.commons.logging.logback.FLEX_OPERATIONS_JOB_LOGGER_NAME
import io.airbyte.commons.storage.GcsLogUploadTarget
import io.airbyte.commons.storage.LogUploadAuthorizationRefreshResult
import io.airbyte.micronaut.runtime.AirbyteConnectorConfig
import io.airbyte.micronaut.runtime.AirbyteContextConfig
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.GcsDownscopedOAuthLogUploadAuthorization
import io.airbyte.workload.api.domain.LogDeliveryMode
import io.airbyte.workload.api.domain.LogUploadAuthorization
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger.ROOT_LOGGER_NAME
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

private val logger = KotlinLogging.logger {}

/** When the init-container selects FLEX delivery, changes only the remote operations-log appender; console and unrelated appenders remain active. */
@Singleton
class FlexLogAppenderInitializer internal constructor(
  private val connectorConfig: AirbyteConnectorConfig,
  private val contextConfig: AirbyteContextConfig,
  private val workloadApiClient: WorkloadApiClient,
  private val loggerContextProvider: () -> LoggerContext,
  private val appenderFactory: (GcsLogUploadTarget, () -> LogUploadAuthorizationRefreshResult) -> Appender<ILoggingEvent>,
) {
  @Inject
  constructor(
    connectorConfig: AirbyteConnectorConfig,
    contextConfig: AirbyteContextConfig,
    workloadApiClient: WorkloadApiClient,
  ) : this(
    connectorConfig = connectorConfig,
    contextConfig = contextConfig,
    workloadApiClient = workloadApiClient,
    loggerContextProvider = { LoggerFactory.getILoggerFactory() as LoggerContext },
    appenderFactory = { target, refresh -> AirbyteFlexLogbackAppender(target, refresh) },
  )

  fun initialize() {
    if (readDeliveryMode() != LogDeliveryMode.FLEX) {
      return
    }

    try {
      val loggerContext = loggerContextProvider()
      val rootLogger = loggerContext.getLogger(ROOT_LOGGER_NAME)
      rootLogger.getAppender(CLOUD_OPERATIONS_JOB_LOGGER_NAME)?.let { standardAppender ->
        rootLogger.detachAppender(standardAppender)
        standardAppender.stop()
      }

      val initialTarget = readInitialTarget() ?: return
      val flexAppender = appenderFactory(initialTarget) { refreshAuthorization() }
      flexAppender.context = loggerContext
      flexAppender.name = FLEX_OPERATIONS_JOB_LOGGER_NAME
      flexAppender.start()
      rootLogger.addAppender(flexAppender)
    } catch (_: Exception) {
      logger.warn { "Unable to initialize FLEX log delivery. Continuing without remote operations-log delivery." }
    }
  }

  private fun readDeliveryMode(): LogDeliveryMode? =
    try {
      MoreMappers.initMapper().readValue(configPath(FileConstants.LOG_DELIVERY_MODE_FILE).toFile(), LogDeliveryMode::class.java)
    } catch (_: Exception) {
      null
    }

  private fun readInitialTarget(): GcsLogUploadTarget? {
    if (!Files.isRegularFile(configPath(FileConstants.LOG_UPLOAD_AUTHORIZATION_FILE))) return null
    return try {
      val authorization =
        MoreMappers
          .initMapper()
          .readValue(configPath(FileConstants.LOG_UPLOAD_AUTHORIZATION_FILE).toFile(), LogUploadAuthorization::class.java)
      authorization.toSharedTarget()?.takeIf { it.isValid() }
    } catch (_: Exception) {
      null
    }
  }

  private fun refreshAuthorization(): LogUploadAuthorizationRefreshResult =
    try {
      workloadApiClient
        .workloadLogUploadAuthorization(contextConfig.workloadId)
        ?.toSharedTarget()
        ?.takeIf { it.isValid() }
        ?.let(LogUploadAuthorizationRefreshResult::Refreshed)
        ?: LogUploadAuthorizationRefreshResult.TerminalFailure
    } catch (e: ApiException) {
      if (e.statusCode in 500..599) {
        LogUploadAuthorizationRefreshResult.RetryableFailure
      } else {
        LogUploadAuthorizationRefreshResult.TerminalFailure
      }
    } catch (_: JsonProcessingException) {
      LogUploadAuthorizationRefreshResult.TerminalFailure
    } catch (_: IOException) {
      LogUploadAuthorizationRefreshResult.RetryableFailure
    } catch (_: Exception) {
      LogUploadAuthorizationRefreshResult.TerminalFailure
    }

  private fun LogUploadAuthorization.toSharedTarget(): GcsLogUploadTarget? =
    when (this) {
      is GcsDownscopedOAuthLogUploadAuthorization ->
        GcsLogUploadTarget(
          accessToken = accessToken,
          expiresAt = expiresAt,
          bucketName = bucketName,
          objectKeyPrefix = objectKeyPrefix,
        )
    }

  private fun configPath(fileName: String): Path = Path.of(connectorConfig.configDir).resolve(fileName)
}
