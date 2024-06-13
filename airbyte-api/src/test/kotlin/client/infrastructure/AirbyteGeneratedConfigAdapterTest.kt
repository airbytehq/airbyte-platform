/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.squareup.moshi.adapter
import io.airbyte.api.client.model.generated.FailAttemptRequest
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.FailureReason
import io.airbyte.config.Metadata
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.openapitools.client.infrastructure.Serializer

internal class AirbyteGeneratedConfigAdapterTest {
  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testGeneratedConfigDeserialization() {
    val startTime = System.currentTimeMillis()
    val request =
      FailAttemptRequest(
        jobId = 1L,
        attemptNumber = 1,
        failureSummary =
          AttemptFailureSummary()
            .withFailures(
              listOf(
                FailureReason()
                  .withMetadata(
                    Metadata()
                      .withAdditionalProperty("from", "connector"),
                  ),
              ),
            ).withPartialSuccess(false),
        standardSyncOutput =
          StandardSyncOutput()
            .withStandardSyncSummary(
              StandardSyncSummary()
                .withStatus(StandardSyncSummary.ReplicationStatus.FAILED)
                .withStartTime(startTime)
                .withAdditionalProperty("connector_type", "source"),
            ),
      )
    val adapter = Serializer.moshi.adapter<FailAttemptRequest>()
    val json = adapter.toJson(request)
    val result = Jsons.deserialize(json)
    assertEquals("connector", result.get("failureSummary").get("failures").first().get("metadata").get("from").asText())
    assertEquals(false, result.get("failureSummary").get("partialSuccess").asBoolean())
    assertEquals(
      StandardSyncSummary.ReplicationStatus.FAILED.value(),
      result.get("standardSyncOutput").get("standardSyncSummary").get("status").asText(),
    )
    assertEquals(startTime, result.get("standardSyncOutput").get("standardSyncSummary").get("startTime").asLong())
    assertEquals("source", result.get("standardSyncOutput").get("standardSyncSummary").get("connector_type").asText())
  }
}
