/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.input

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConnectionContext
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.input.ReplicationInputMapperTest.Fixtures.getActivityInputForSourceConfig
import io.airbyte.workers.models.ReplicationActivityInput
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream

class ReplicationInputMapperTest {
  @ParameterizedTest
  @MethodSource("useFileTransferFormat")
  fun `map activity input to repl input`(replicationActivityInput: ReplicationActivityInput) {
    val mapper = ReplicationInputMapper()

    val replicationInput = mapper.toReplicationInput(replicationActivityInput)

    assertEquals(replicationActivityInput.sourceConfiguration, replicationInput.sourceConfiguration)
    assertEquals(replicationActivityInput.destinationConfiguration, replicationInput.destinationConfiguration)
    assertEquals(replicationActivityInput.namespaceDefinition, replicationInput.namespaceDefinition)
    assertEquals(replicationActivityInput.namespaceFormat, replicationInput.namespaceFormat)
    assertEquals(replicationActivityInput.prefix, replicationInput.prefix)
    assertEquals(replicationActivityInput.sourceId, replicationInput.sourceId)
    assertEquals(replicationActivityInput.destinationId, replicationInput.destinationId)
    assertEquals(replicationActivityInput.syncResourceRequirements, replicationInput.syncResourceRequirements)
    assertEquals(replicationActivityInput.workspaceId, replicationInput.workspaceId)
    assertEquals(replicationActivityInput.connectionId, replicationInput.connectionId)
    assertEquals(replicationActivityInput.isReset, replicationInput.isReset)
    assertEquals(replicationActivityInput.jobRunConfig, replicationInput.jobRunConfig)
    assertEquals(replicationActivityInput.sourceLauncherConfig, replicationInput.sourceLauncherConfig)
    assertEquals(replicationActivityInput.destinationLauncherConfig, replicationInput.destinationLauncherConfig)
    assertEquals(replicationActivityInput.signalInput, replicationInput.signalInput)
    assertEquals(replicationActivityInput.sourceConfiguration, replicationInput.sourceConfiguration)
    assertEquals(replicationActivityInput.destinationConfiguration, replicationInput.destinationConfiguration)
    assertEquals(replicationActivityInput.connectionContext, replicationInput.connectionContext)
    assertEquals(true, replicationInput.useFileTransfer)
  }

  @Test
  fun `map activity input to repl input file transfer default`() {
    val replicationActivityInputSimpleFileTransfer = getActivityInputForSourceConfig(Jsons.jsonNode(mapOf("no" to "file transfer")))

    val mapper = ReplicationInputMapper()

    val replicationInput = mapper.toReplicationInput(replicationActivityInputSimpleFileTransfer)

    assertEquals(false, replicationInput.useFileTransfer)
  }

  companion object {
    @JvmStatic
    fun useFileTransferFormat(): Stream<Arguments> =
      Stream.of(
        Arguments.of(Fixtures.replicationActivityInputSimpleFileTransfer),
        Arguments.of(Fixtures.replicationActivityInputNewFileTransfer),
      )
  }

  object Fixtures {
    fun getActivityInputForSourceConfig(sourceConfig: JsonNode): ReplicationActivityInput =
      ReplicationActivityInput(
        UUID.randomUUID(),
        UUID.randomUUID(),
        sourceConfig,
        Jsons.jsonNode(mapOf("destination" to "configuration")),
        JobRunConfig().withJobId("123").withAttemptId(0L),
        IntegrationLauncherConfig().withDockerImage("source:dockertag"),
        IntegrationLauncherConfig().withDockerImage("destination:dockertag"),
        SyncResourceRequirements(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        "unused",
        false,
        JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT,
        "unused",
        "unused",
        null,
        ConnectionContext().withOrganizationId(UUID.randomUUID()),
        null,
        emptyList(),
      )

    val replicationActivityInputSimpleFileTransfer =
      getActivityInputForSourceConfig(Jsons.jsonNode(mapOf("use_file_transfer" to true)))

    val replicationActivityInputNewFileTransfer =
      getActivityInputForSourceConfig(Jsons.jsonNode(mapOf("delivery_method" to mapOf("delivery_type" to "use_file_transfer"))))
  }
}
