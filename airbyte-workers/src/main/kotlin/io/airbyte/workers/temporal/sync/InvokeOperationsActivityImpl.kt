/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.commons.io.LineGobbler
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.logging.MdcScope
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.config.OperatorWebhookInput
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.WebhookOperationSummary
import io.airbyte.persistence.job.models.JobRunConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.util.CollectionUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path

private val logger = KotlinLogging.logger { }

@Singleton
class InvokeOperationsActivityImpl(
  private val webhookOperationActivity: WebhookOperationActivity,
  private val logClientManager: LogClientManager,
  @Named("workspaceRoot") private val workspaceRoot: Path,
) : InvokeOperationsActivity {
  companion object {
    const val SECTION_NAME = "POST REPLICATION OPERATIONS"
  }

  override fun invokeOperations(
    operations: List<StandardSyncOperation>,
    syncInput: StandardSyncInput,
    jobRunConfig: JobRunConfig,
  ): WebhookOperationSummary {
    val webhookOperationSummary = WebhookOperationSummary()
    MdcScope.Builder()
      .setExtraMdcEntries(LogSource.PLATFORM.toMdc())
      .build().use { _ ->
        try {
          logClientManager.setJobMdc(TemporalUtils.getJobRoot(workspaceRoot, jobRunConfig.jobId, jobRunConfig.attemptId))
          logger.info { LineGobbler.formatStartSection(SECTION_NAME) }

          if (CollectionUtils.isNotEmpty(operations)) {
            logger.info { "Invoking ${operations.size} post-replication operation(s)..." }
            for (standardSyncOperation in operations) {
              if (standardSyncOperation.operatorType == StandardSyncOperation.OperatorType.WEBHOOK) {
                logger.info { "Invoking webhook operation ${standardSyncOperation.operatorWebhook.webhookConfigId}..." }
                logger.debug { "Webhook operation input for connection $standardSyncOperation" }
                val success: Boolean =
                  webhookOperationActivity
                    .invokeWebhook(
                      OperatorWebhookInput()
                        .withExecutionUrl(standardSyncOperation.operatorWebhook.executionUrl)
                        .withExecutionBody(standardSyncOperation.operatorWebhook.executionBody)
                        .withWebhookConfigId(standardSyncOperation.operatorWebhook.webhookConfigId)
                        .withWorkspaceWebhookConfigs(syncInput.webhookOperationConfigs)
                        .withConnectionContext(syncInput.connectionContext),
                    )
                logger.info {
                  "Webhook ${standardSyncOperation.operatorWebhook.webhookConfigId} completed ${if (success) "successfully" else "unsuccessfully"}"
                }
                if (success) {
                  webhookOperationSummary.successes.add(standardSyncOperation.operatorWebhook.webhookConfigId)
                } else {
                  webhookOperationSummary.failures.add(standardSyncOperation.operatorWebhook.webhookConfigId)
                }
              } else {
                logger.warn { "Unsupported operation type '${standardSyncOperation.operatorType}' found.  Skipping operation..." }
              }
            }
          } else {
            logger.info { "No post-replication operation(s) to perform." }
          }
        } finally {
          logger.info { LineGobbler.formatEndSection(SECTION_NAME) }
          logClientManager.setJobMdc(null)
        }
      }
    return webhookOperationSummary
  }
}
