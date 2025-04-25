/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConnectionContext
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID

internal class GenerateReplicationActivityInputActivityTest {
  @ParameterizedTest
  @MethodSource("signalInputProvider")
  fun testGeneratingReplicationActivityInput(signalInput: String?) {
    val connectionCtx = mockk<ConnectionContext>()
    val connectionUUID = UUID.randomUUID()
    val destinationConfigurationJson = Jsons.jsonNode("{}")
    val destinationUUID = UUID.randomUUID()
    val namespaceDefinitionType = JobSyncConfig.NamespaceDefinitionType.SOURCE
    val namespaceFormatString = "test_format"
    val prefixString = "test_prefix"
    val securityTokens = listOf("token1", "token2")
    val shouldOmitFieTransferEnvVar = true
    val shouldIncludeFiles = true
    val shouldReset = true
    val sourceConfigurationJson = Jsons.jsonNode("{}")
    val sourceUUID = UUID.randomUUID()
    val syncResourceReqs = mockk<SyncResourceRequirements>()
    val workspaceUUID = UUID.randomUUID()

    val destinationLauncherConfig = mockk<IntegrationLauncherConfig>()
    val jobRunConfig = mockk<JobRunConfig>()
    val refreshSchemaOutput = mockk<RefreshSchemaActivityOutput>()
    val sourceLauncherConfig = mockk<IntegrationLauncherConfig>()
    val syncInput =
      mockk<StandardSyncInput> {
        every { connectionContext } returns connectionCtx
        every { connectionId } returns connectionUUID
        every { destinationConfiguration } returns destinationConfigurationJson
        every { destinationId } returns destinationUUID
        every { includesFiles } returns shouldIncludeFiles
        every { isReset } returns shouldReset
        every { namespaceDefinition } returns namespaceDefinitionType
        every { networkSecurityTokens } returns securityTokens
        every { prefix } returns prefixString
        every { namespaceFormat } returns namespaceFormatString
        every { omitFileTransferEnvVar } returns shouldOmitFieTransferEnvVar
        every { sourceConfiguration } returns sourceConfigurationJson
        every { sourceId } returns sourceUUID
        every { syncResourceRequirements } returns syncResourceReqs
        every { workspaceId } returns workspaceUUID
      }
    val taskQueue = "test-task-queue"
    val activity = GenerateReplicationActivityInputActivityImpl()

    val input =
      activity.generate(
        syncInput = syncInput,
        jobRunConfig = jobRunConfig,
        sourceLauncherConfig = sourceLauncherConfig,
        destinationLauncherConfig = destinationLauncherConfig,
        taskQueue = taskQueue,
        refreshSchemaOutput = refreshSchemaOutput,
        signalInput = signalInput,
      )

    assertEquals(connectionCtx, input.connectionContext)
    assertEquals(connectionUUID, input.connectionId)
    assertEquals(destinationConfigurationJson, input.destinationConfiguration)
    assertEquals(destinationUUID, input.destinationId)
    assertEquals(destinationLauncherConfig, input.destinationLauncherConfig)
    assertEquals(jobRunConfig, input.jobRunConfig)
    assertEquals(namespaceDefinitionType, input.namespaceDefinition)
    assertEquals(namespaceFormatString, input.namespaceFormat)
    assertEquals(prefixString, input.prefix)
    assertEquals(refreshSchemaOutput, input.schemaRefreshOutput)
    assertEquals(securityTokens, input.networkSecurityTokens)
    assertEquals(shouldOmitFieTransferEnvVar, input.omitFileTransferEnvVar)
    assertEquals(shouldIncludeFiles, input.includesFiles)
    assertEquals(shouldReset, input.isReset)
    assertEquals(signalInput, input.signalInput)
    assertEquals(sourceConfigurationJson, input.sourceConfiguration)
    assertEquals(sourceLauncherConfig, input.sourceLauncherConfig)
    assertEquals(sourceUUID, input.sourceId)
    assertEquals(syncResourceReqs, input.syncResourceRequirements)
    assertEquals(taskQueue, input.taskQueue)
    assertEquals(workspaceUUID, input.workspaceId)
  }

  companion object {
    @JvmStatic
    fun signalInputProvider(): List<String?> = listOf("{\"signal-input\":\"foo\"}", null)
  }
}
