/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2Input
import io.airbyte.config.CatalogDiff
import io.airbyte.config.ConnectionContext
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.WebhookOperationSummary
import io.airbyte.workers.models.PostprocessCatalogInput
import io.airbyte.workers.models.PostprocessCatalogOutput
import io.airbyte.workers.temporal.activities.GetConnectionContextInput
import io.airbyte.workers.temporal.activities.GetConnectionContextOutput
import io.airbyte.workers.temporal.activities.GetWebhookConfigInput
import io.airbyte.workers.temporal.activities.GetWebhookConfigOutput
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogHelperActivity
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class SyncWorkflowV2Test {
  private val configFetchActivity: ConfigFetchActivity = mockk(relaxed = true)
  private val discoverCatalogHelperActivity: DiscoverCatalogHelperActivity = mockk(relaxed = true)
  private val invokeOperationsActivity: InvokeOperationsActivity = mockk(relaxed = true)

  private val syncWorkflowV2: SyncWorkflowV2Impl =
    spyk(SyncWorkflowV2Impl(configFetchActivity, discoverCatalogHelperActivity, invokeOperationsActivity))

  private val connectionId = UUID.randomUUID()
  private val jobId = 123L
  private val attemptNumber = 1L
  private val sourceId = UUID.randomUUID()

  private val input =
    SyncWorkflowV2Input(
      connectionId = connectionId,
      jobId = jobId,
      attemptNumber = attemptNumber,
      sourceId = sourceId,
    )

  private val discoverConnectorJobOutput =
    ConnectorJobOutput()
      .withDiscoverCatalogId(UUID.randomUUID())
      .withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)

  @BeforeEach
  fun init() {
    every { syncWorkflowV2.runDiscoverCommand(sourceId, jobId.toString(), attemptNumber) } returns discoverConnectorJobOutput
  }

  @Test
  fun testSyncWorkflowV2StopIfNeeded() {
    every { configFetchActivity.getStatus(connectionId) } returns java.util.Optional.of(ConnectionStatus.INACTIVE)

    val output = syncWorkflowV2.run(input)

    assertEquals(StandardSyncSummary.ReplicationStatus.CANCELLED, output.standardSyncSummary.status)
  }

  @Test
  fun testSyncWorkflowV2() {
    val catalogDiff =
      CatalogDiff()
        .withAdditionalProperty("catalog", "diff")
    every { discoverCatalogHelperActivity.postprocess(PostprocessCatalogInput(discoverConnectorJobOutput.discoverCatalogId, connectionId)) } returns
      PostprocessCatalogOutput
        .Builder()
        .diff(catalogDiff)
        .build()
    every { configFetchActivity.getStatus(connectionId) } returns java.util.Optional.of(ConnectionStatus.ACTIVE)

    val webhookSummary = WebhookOperationSummary().withAdditionalProperty("operation", "summary")
    val replicateResult =
      ConnectorJobOutput()
        .withReplicate(
          StandardSyncOutput()
            .withAdditionalProperty("replicate", "success")
            .withWebhookOperationSummary(webhookSummary),
        )
    every {
      syncWorkflowV2.runReplicationCommand(
        connectionId = connectionId,
        jobId = jobId.toString(),
        attemptNumber = attemptNumber,
        appliedCatalogDiff = catalogDiff,
      )
    } returns replicateResult

    val connectionContext = ConnectionContext().withAdditionalProperty("context", "success")
    every { configFetchActivity.getConnectionContext(GetConnectionContextInput(connectionId)) } returns
      GetConnectionContextOutput(connectionContext)

    val operations = listOf(StandardSyncOperation().withAdditionalProperty("operation", "in a list"))
    val webhookOperationConfig = Jsons.jsonNode(mapOf("operation" to "config"))

    every { configFetchActivity.getWebhookConfig(GetWebhookConfigInput(jobId)) } returns
      GetWebhookConfigOutput(
        operations = operations,
        webhookOperationConfigs = webhookOperationConfig,
      )

    every {
      invokeOperationsActivity.invokeOperationsV2(
        operations,
        webhookOperationConfig,
        connectionContext,
        jobId.toString(),
        attemptNumber,
      )
    } returns
      webhookSummary

    val output = syncWorkflowV2.run(input)

    assertEquals(replicateResult.replicate, output)
  }
}
