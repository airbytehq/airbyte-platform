/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.ConnectionSearch
import io.airbyte.api.server.generated.models.DestinationSearch
import io.airbyte.api.server.generated.models.SourceSearch
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class ConfigSearchRuleTest {
  private val resolver = mockk<AuthenticationHeaderResolver>()
  private val rule = ConfigSearchRule(resolver)
  private val workspaceId = UUID.randomUUID()
  private val scopedCaller = Authentication.build("embedded", emptyList(), mapOf(TokenScopeClaim.CLAIM_ID to TokenScopeClaim(workspaceId.toString())))

  @Test
  fun `unscoped caller keeps authenticated search behavior`() {
    val input = rule.readInput(arrayOf(SourceSearch()))

    assertDoesNotThrow { rule.authorize(Authentication.build("user"), input) }
    verify(exactly = 0) { resolver.resolveWorkspace(any()) }
  }

  @Test
  fun `scoped caller can search its workspace`() {
    val input = rule.readInput(arrayOf(SourceSearch(workspaceId = workspaceId)))
    val references = mapOf(AuthenticationId.WORKSPACE_ID.httpHeader to workspaceId.toString())
    every { resolver.resolveWorkspace(references) } returns listOf(workspaceId)

    assertDoesNotThrow { rule.authorize(scopedCaller, input) }
  }

  @Test
  fun `scoped caller cannot search without a resolved workspace`() {
    val input = rule.readInput(arrayOf(DestinationSearch()))
    every { resolver.resolveWorkspace(emptyMap()) } returns null

    assertThrows<ForbiddenProblem> { rule.authorize(scopedCaller, input) }
  }

  @Test
  fun `scoped caller cannot search a different workspace`() {
    val otherWorkspace = UUID.randomUUID()
    val input = rule.readInput(arrayOf(DestinationSearch(destinationId = UUID.randomUUID()))) as ConfigSearchInput
    every { resolver.resolveWorkspace(input.references) } returns listOf(otherWorkspace)

    assertThrows<ForbiddenProblem> { rule.authorize(scopedCaller, input) }
  }

  @Test
  fun `connection selectors are resolved together`() {
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()

    val input = rule.readInput(arrayOf(ConnectionSearch(connectionId = connectionId, sourceId = sourceId, destinationId = destinationId)))

    assertEquals(
      mapOf(
        AuthenticationId.CONNECTION_ID.httpHeader to connectionId.toString(),
        AuthenticationId.SOURCE_ID.httpHeader to sourceId.toString(),
        AuthenticationId.DESTINATION_ID_.httpHeader to destinationId.toString(),
      ),
      (input as ConfigSearchInput).references,
    )
  }
}
