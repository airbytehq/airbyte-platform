/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOperation
import io.airbyte.persistence.job.models.JobRunConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.UUID

internal class InvokeOperationsActivityTest {
  @Test
  internal fun testInvokingWebhookOperations() {
    val operations =
      listOf(
        mockk<StandardSyncOperation> {
          every { operatorType } returns StandardSyncOperation.OperatorType.WEBHOOK
          every { operatorWebhook } returns
            mockk {
              every { executionBody } returns "body=value"
              every { executionUrl } returns "http://localhost"
              every { webhookConfigId } returns UUID.randomUUID()
            }
        },
      )
    val jobRunConfig =
      mockk<JobRunConfig> {
        every { jobId } returns "1"
        every { attemptId } returns 0
      }
    val syncInput =
      mockk<StandardSyncInput> {
        every { webhookOperationConfigs } returns Jsons.jsonNode(mapOf<String, String>())
        every { connectionContext } returns mockk()
      }
    val webhookOperationActivity =
      mockk<WebhookOperationActivity> {
        every { invokeWebhook(any()) } returns true
      }
    val logClientManager =
      mockk<LogClientManager> {
        every { setJobMdc(any()) } returns Unit
      }
    val workspaceRoot = Path.of("/workspace")

    val invokeOperationsActivity =
      InvokeOperationsActivityImpl(
        webhookOperationActivity = webhookOperationActivity,
        logClientManager = logClientManager,
        workspaceRoot = workspaceRoot,
      )

    val summary =
      invokeOperationsActivity.invokeOperations(
        operations = operations,
        jobRunConfig = jobRunConfig,
        syncInput = syncInput,
      )

    assertEquals(1, summary.successes.size)
  }

  @Test
  internal fun testInvokingWebhookOperationsFailure() {
    val operations =
      listOf(
        mockk<StandardSyncOperation> {
          every { operatorType } returns StandardSyncOperation.OperatorType.WEBHOOK
          every { operatorWebhook } returns
            mockk {
              every { executionBody } returns "body=value"
              every { executionUrl } returns "http://localhost"
              every { webhookConfigId } returns UUID.randomUUID()
            }
        },
      )
    val jobRunConfig =
      mockk<JobRunConfig> {
        every { jobId } returns "1"
        every { attemptId } returns 0
      }
    val syncInput =
      mockk<StandardSyncInput> {
        every { webhookOperationConfigs } returns Jsons.jsonNode(mapOf<String, String>())
        every { connectionContext } returns mockk()
      }
    val webhookOperationActivity =
      mockk<WebhookOperationActivity> {
        every { invokeWebhook(any()) } returns false
      }
    val logClientManager =
      mockk<LogClientManager> {
        every { setJobMdc(any()) } returns Unit
      }
    val workspaceRoot = Path.of("/workspace")

    val invokeOperationsActivity =
      InvokeOperationsActivityImpl(
        webhookOperationActivity = webhookOperationActivity,
        logClientManager = logClientManager,
        workspaceRoot = workspaceRoot,
      )

    val summary =
      invokeOperationsActivity.invokeOperations(
        operations = operations,
        jobRunConfig = jobRunConfig,
        syncInput = syncInput,
      )

    assertEquals(1, summary.failures.size)
  }

  @Test
  internal fun testInvokingNoWebhookOperations() {
    val operations = listOf<StandardSyncOperation>()
    val jobRunConfig =
      mockk<JobRunConfig> {
        every { jobId } returns "1"
        every { attemptId } returns 0
      }
    val syncInput =
      mockk<StandardSyncInput> {
        every { webhookOperationConfigs } returns Jsons.jsonNode(mapOf<String, String>())
        every { connectionContext } returns mockk()
      }
    val webhookOperationActivity =
      mockk<WebhookOperationActivity> {
        every { invokeWebhook(any()) } returns true
      }
    val logClientManager =
      mockk<LogClientManager> {
        every { setJobMdc(any()) } returns Unit
      }
    val workspaceRoot = Path.of("/workspace")

    val invokeOperationsActivity =
      InvokeOperationsActivityImpl(
        webhookOperationActivity = webhookOperationActivity,
        logClientManager = logClientManager,
        workspaceRoot = workspaceRoot,
      )

    val summary =
      invokeOperationsActivity.invokeOperations(
        operations = operations,
        jobRunConfig = jobRunConfig,
        syncInput = syncInput,
      )

    assertEquals(0, summary.successes.size)
    assertEquals(0, summary.failures.size)
    verify(exactly = 0) { webhookOperationActivity.invokeWebhook(any()) }
  }
}
