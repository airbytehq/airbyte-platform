/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.RunCheckCommandRequest
import io.airbyte.api.server.generated.models.RunConnectCommandRequest
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID

class ConnectCommandRuleTest {
  private val workspaceId = UUID.randomUUID()
  private val owner = WorkspaceAndOrganization(workspaceId, UUID.randomUUID())
  private val data = mockk<AuthorizationDataService>()
  private val permissions = mockk<OperationPermissions>(relaxed = true)
  private val rule = ConnectCommandRule(data, permissions)

  private fun request(
    id: String = "cmd",
    host: String = "db.example.com",
    port: Int = 5432,
  ) = RunConnectCommandRequest(id = id, workspaceId = workspaceId, host = host, port = port)

  @Test
  fun `readInput returns the workspace of a valid request`() {
    assertEquals(ConnectCommandInput(workspaceId), rule.readInput(arrayOf(request())))
  }

  @ParameterizedTest
  @ValueSource(ints = [1, 5432, 65535])
  fun `readInput accepts ports in range`(port: Int) {
    assertEquals(ConnectCommandInput(workspaceId), rule.readInput(arrayOf(request(port = port))))
  }

  @ParameterizedTest
  @ValueSource(ints = [-1, 0, 65536, 100000])
  fun `readInput rejects ports out of range`(port: Int) {
    assertThrows<BadRequestProblem> { rule.readInput(arrayOf(request(port = port))) }
  }

  @ParameterizedTest
  @ValueSource(strings = ["db.example.com", "localhost", "host.docker.internal", "10.0.0.5", "8.8.8.8", "::1", "[2001:db8::1]"])
  fun `readInput accepts hostnames and ip literals`(host: String) {
    assertEquals(ConnectCommandInput(workspaceId), rule.readInput(arrayOf(request(host = host))))
  }

  @ParameterizedTest
  @ValueSource(
    strings = ["", " ", "\t", "999.0.0.1", "256.1.1.1", "1:2:3:4:5:6:7:8:9", "fe80::1%nope", "http://db.example.com", "db example.com"],
  )
  fun `readInput rejects blank, malformed and non-host values`(host: String) {
    assertThrows<BadRequestProblem> { rule.readInput(arrayOf(request(host = host))) }
  }

  @Test
  fun `readInput rejects a blank id`() {
    assertThrows<BadRequestProblem> { rule.readInput(arrayOf(request(id = " "))) }
  }

  @Test
  fun `readInput rejects missing or wrongly typed bodies`() {
    assertThrows<BadRequestProblem> { rule.readInput(arrayOf(null)) }
    assertThrows<BadRequestProblem> { rule.readInput(emptyArray()) }
    assertThrows<BadRequestProblem> { rule.readInput(arrayOf(RunCheckCommandRequest(id = "cmd"))) }
    assertThrows<BadRequestProblem> { rule.readInput(arrayOf(request(), request())) }
  }

  @Test
  fun `authorize requires runner or embedded end user on the workspace`() {
    val caller = mockk<Authentication>()
    every { data.workspaceAndOrganization(workspaceId) } returns owner

    rule.authorize(caller, ConnectCommandInput(workspaceId))

    verify {
      permissions.requireWorkspaces(
        caller,
        listOf(owner),
        setOf(AuthRoleConstants.WORKSPACE_RUNNER, AuthRoleConstants.EMBEDDED_END_USER),
        OperationCaller.entries.toSet(),
      )
    }
  }
}
