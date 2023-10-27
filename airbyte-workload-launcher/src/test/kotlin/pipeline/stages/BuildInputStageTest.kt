package io.airbyte.workload.launcher.pipeline.stages

import com.fasterxml.jackson.databind.node.POJONode
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.mocks.LauncherInputMessage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.serde.PayloadDeserializer
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test

class BuildInputStageTest {
  @Test
  fun `parses input and hydrates`() {
    val msgStr = "foo"
    val sourceConfig = POJONode("bar")
    val destConfig = POJONode("baz")
    val replInput =
      ReplicationInput()
        .withSourceConfiguration(sourceConfig)
        .withDestinationConfiguration(destConfig)

//    val secretsHydrator: SecretsHydrator = mockk()
    val deserializer: PayloadDeserializer = mockk()
    every { deserializer.toReplicationInput(msgStr) } returns replInput
//    every { secretsHydrator.hydrate(any()) } returns POJONode("")

    val stage =
      BuildInputStage(
//      secretsHydrator,
        deserializer,
      )
    val io = LaunchStageIO(msg = LauncherInputMessage("1", msgStr))

    val result = stage.applyStage(io)

    verify {
      deserializer.toReplicationInput(msgStr)
//      secretsHydrator.hydrate(sourceConfig)
//      secretsHydrator.hydrate(destConfig)
    }

    assert(result.replicationInput == replInput)
  }
}
