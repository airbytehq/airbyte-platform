/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.ConnectionContext
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.WebhookOperationSummary
import io.airbyte.persistence.job.models.JobRunConfig
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface InvokeOperationsActivity {
  @ActivityMethod
  fun invokeOperations(
    operations: List<StandardSyncOperation>,
    syncInput: StandardSyncInput,
    jobRunConfig: JobRunConfig,
  ): WebhookOperationSummary

  @ActivityMethod
  fun invokeOperationsV2(
    operations: List<StandardSyncOperation>,
    webhookOperationConfigs: JsonNode?,
    connectionContext: ConnectionContext?,
    jobId: String,
    attemptId: Long,
  ): WebhookOperationSummary
}
