/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling

import io.airbyte.commons.json.Jsons
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectorCommandInputTest {
  @Test
  fun `test check command input serde`() {
    val input =
      CheckCommandInput(
        input =
          CheckCommandInput.CheckConnectionInput(
            jobRunConfig = JobRunConfig().withJobId("test"),
            integrationLauncherConfig = IntegrationLauncherConfig().withJobId("more test"),
            checkConnectionInput = StandardCheckConnectionInput().withActorId(UUID.randomUUID()),
          ),
      )

    val result = Jsons.deserialize(Jsons.serialize(input), ConnectorCommandInput::class.java)
    assertEquals(input, result)
  }

  @Test
  fun `test discover command input serde`() {
    val input =
      DiscoverCommandInput(
        input =
          DiscoverCommandInput.DiscoverCatalogInput(
            jobRunConfig = JobRunConfig().withAttemptId(1L),
            integrationLauncherConfig = IntegrationLauncherConfig().withDockerImage("bad image"),
            discoverCatalogInput = StandardDiscoverCatalogInput().withManual(true),
          ),
      )

    val result = Jsons.deserialize(Jsons.serialize(input), ConnectorCommandInput::class.java)
    assertEquals(input, result)
  }

  @Test
  fun `test spec command input serde`() {
    val input =
      SpecCommandInput(
        input =
          SpecCommandInput.SpecInput(
            jobRunConfig = JobRunConfig().withAttemptId(1L),
            integrationLauncherConfig = IntegrationLauncherConfig().withDockerImage("bad image"),
          ),
      )

    val result = Jsons.deserialize(Jsons.serialize(input), ConnectorCommandInput::class.java)
    assertEquals(input, result)
  }
}
