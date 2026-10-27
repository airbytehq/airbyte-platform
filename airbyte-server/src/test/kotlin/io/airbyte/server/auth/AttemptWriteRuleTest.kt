/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.AttemptStats
import io.airbyte.api.server.generated.models.SaveStatsRequestBody
import io.airbyte.api.server.generated.models.SaveStreamAttemptMetadataRequestBody
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

class AttemptWriteRuleTest {
  private val stats = SaveAttemptStatsRule(mockk(), mockk())
  private val metadata = SaveAttemptStreamMetadataRule(mockk(), mockk())

  @Test
  fun `stats parses job attempt and optional connection IDs`() {
    val connection = UUID.randomUUID()
    val body = SaveStatsRequestBody(jobId = 71L, attemptNumber = 0, stats = AttemptStats(), connectionId = connection)
    assertEquals(SaveAttemptStatsInput(71, 0, Optional.of(connection)), stats.readInput(arrayOf(body)))
    assertEquals(Optional.empty<UUID>(), stats.readInput(arrayOf(body.copy(connectionId = null))).connectionId)
  }

  @Test
  fun `metadata parses composite attempt IDs`() {
    val body = SaveStreamAttemptMetadataRequestBody(jobId = 71L, attemptNumber = 0)
    assertEquals(SaveAttemptStreamMetadataInput(71, 0), metadata.readInput(arrayOf(body)))
  }

  @Test
  fun `both rules reject absent and mistyped inputs`() {
    for (args in listOf<Array<out Any?>>(
      emptyArray(),
      arrayOf(null),
      arrayOf("body"),
    )) {
      assertThrows<BadRequestProblem> { stats.readInput(args) }
    }
    for (args in listOf<Array<out Any?>>(
      emptyArray(),
      arrayOf(null),
      arrayOf("body"),
    )) {
      assertThrows<BadRequestProblem> { metadata.readInput(args) }
    }
  }
}
