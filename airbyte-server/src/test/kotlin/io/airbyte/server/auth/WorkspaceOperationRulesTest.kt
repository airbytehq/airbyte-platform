/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class WorkspaceOperationRulesTest {
  @Test
  fun `connection tombstone read resolves its actual saved source workspace without an active workspace lookup`() {
    val workspace = UUID(0, 1)
    val organization = UUID(0, 2)
    val connection = UUID(0, 3)
    val source = UUID(0, 4)
    val workspaces = mockk<WorkspaceService>()
    val connections = mockk<ConnectionService>()
    val actors = mockk<ActorRepository>()
    every { connections.getStandardSync(connection) } returns
      StandardSync().withConnectionId(connection).withSourceId(source).withDestinationId(UUID(0, 5))
    every { actors.findSourceWorkspaceId(source) } returns Optional.of(workspace)
    every { workspaces.getStandardWorkspaceNoSecrets(workspace, false) } throws
      ConfigNotFoundException(ConfigNotFoundType.STANDARD_WORKSPACE, workspace)
    every { workspaces.getStandardWorkspaceNoSecrets(workspace, true) } returns
      StandardWorkspace().withWorkspaceId(workspace).withOrganizationId(organization).withTombstone(true)
    val data =
      AuthorizationDataService(
        workspaces,
        mockk(),
        mockk(),
        connections,
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        mockk(),
        actors,
        mockk(),
        mockk(),
        mockk(),
      )
    val rule = WorkspaceTombstoneConnectionRule(data, OperationPermissions(mockk(), RoleResolver(mockk(), mockk(), mockk(), mockk()), false))
    val input = rule.readInput(arrayOf(ConnectionIdRequestBody(connection)))
    assertDoesNotThrow { rule.authorize(Authentication.build("caller"), input) }
    verify(exactly = 0) { workspaces.getStandardWorkspaceNoSecrets(workspace, false) }
    verify(exactly = 0) { workspaces.getWorkspaceWithSecrets(any(), any()) }
  }
}
