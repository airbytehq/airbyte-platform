/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.ConnectionEventIdRequestBody
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.UUID

class GetConnectionEventRuleTest {
  private val rule = GetConnectionEventRule(mockk(), mockk())

  @Test
  fun `reads event identifier from generated body`() {
    val eventId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    val body = ConnectionEventIdRequestBody(eventId, connectionId)
    val input = rule.readInput(arrayOf(body))

    assertEquals(GetConnectionEventInput(eventId, connectionId), input)
  }

  @Test
  fun `invalid direct invocation input fails before authorization`() {
    for (args in listOf(
      emptyArray<Any?>(),
      arrayOf<Any?>(null),
      arrayOf("invalid"),
    )) {
      assertThrows(BadRequestProblem::class.java) { rule.readInput(args) }
    }
  }
}
