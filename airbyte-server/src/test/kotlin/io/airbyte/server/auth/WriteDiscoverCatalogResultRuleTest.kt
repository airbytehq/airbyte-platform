/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaWriteRequestBody
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class WriteDiscoverCatalogResultRuleTest {
  private val rule = WriteDiscoverCatalogResultRule(mockk(), mockk())

  @Test
  fun `rule snapshots the parsed source identifier instead of retaining the arguments`() {
    val id = UUID.randomUUID()
    val body = SourceDiscoverSchemaWriteRequestBody(catalog = AirbyteCatalog(emptyList()), sourceId = id)
    val arguments = arrayOf<Any?>(body)
    val input = rule.readInput(arguments)
    arguments[0] = body.copy(sourceId = UUID.randomUUID())
    assertEquals(id, input.sourceId)
  }

  @Test
  fun `rule rejects absent or incorrectly typed request arguments`() {
    for (arguments in listOf<Array<out Any?>>(
      emptyArray(),
      arrayOf(null),
      arrayOf("source"),
      arrayOf(SourceDiscoverSchemaWriteRequestBody(AirbyteCatalog(emptyList()))),
    )) {
      assertThrows<BadRequestProblem> { rule.readInput(arguments) }
    }
  }
}
