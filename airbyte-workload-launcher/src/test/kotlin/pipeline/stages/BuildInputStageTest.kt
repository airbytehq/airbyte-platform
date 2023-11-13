/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import com.fasterxml.jackson.databind.node.POJONode
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.LauncherInput
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
    val replActivityInput = ReplicationActivityInput()
    val replInput =
      ReplicationInput()
        .withSourceConfiguration(sourceConfig)
        .withDestinationConfiguration(destConfig)

    val replicationInputHydrator: ReplicationInputHydrator = mockk()
    val deserializer: PayloadDeserializer = mockk()
    every { deserializer.toReplicationActivityInput(msgStr) } returns replActivityInput
    every { replicationInputHydrator.getHydratedReplicationInput(replActivityInput) } returns replInput

    val stage =
      BuildInputStage(
        replicationInputHydrator,
        deserializer,
      )
    val io = LaunchStageIO(msg = LauncherInput("1", msgStr))

    val result = stage.applyStage(io)

    verify {
      deserializer.toReplicationActivityInput(msgStr)
      replicationInputHydrator.getHydratedReplicationInput(replActivityInput)
    }

    assert(result.replicationInput == replInput)
  }
}
