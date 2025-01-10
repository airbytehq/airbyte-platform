package io.airbyte.commons.server.scheduler

import io.airbyte.commons.temporal.JobMetadata
import io.airbyte.commons.temporal.TemporalResponse
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.JobConfig.ConfigType
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.UUID

class SynchronousResponseTest {
  @Test
  fun `test that a run   without output is failed`() {
    val response =
      SynchronousResponse.fromTemporalResponse<String>(
        TemporalResponse.success(ConnectorJobOutput(), JobMetadata(true, Path.of("logPath"))),
        { _ -> null },
        UUID.randomUUID(),
        ConfigType.DISCOVER_SCHEMA,
        UUID.randomUUID(),
        1L,
        2L,
      )

    assertFalse(response.isSuccess)
  }
}
