package io.airbyte.workers.input

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConnectionContext
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.input.ReplicationInputMapperTest.Fixtures.replicationActivityInput
import io.airbyte.workers.models.ReplicationActivityInput
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class ReplicationInputMapperTest {
  @Test
  fun `map activity input to repl input`() {
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
  }

  object Fixtures {
    val replicationActivityInput =
      ReplicationActivityInput(
        UUID.randomUUID(),
        UUID.randomUUID(),
        Jsons.jsonNode(mapOf("source" to "configuration")),
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
      )
  }
}
