/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling

import io.airbyte.commons.json.Jsons
import io.airbyte.config.CatalogDiff
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectorCommandWorkflowTest {
  @Test
  fun `appliedCatalogDiff sets the appliedCatalogDiff property`() {
    val replicationApiInput =
      ReplicationCommandApiInput.ReplicationApiInput(
        connectionId = UUID.randomUUID(),
        jobId = "123",
        attemptId = 1,
        appliedCatalogDiff =
          CatalogDiff().withAdditionalProperty(
            "key",
            "value",
          ),
      )

    val replicationCommandApiInput =
      ReplicationCommandApiInput(
        input = replicationApiInput,
      )

    val serializedApiInput = Jsons.serialize(replicationCommandApiInput)
    val deserializedApiInput = Jsons.deserialize(serializedApiInput, ReplicationCommandApiInput::class.java)

    Assertions.assertEquals(replicationCommandApiInput, deserializedApiInput)
  }
}
