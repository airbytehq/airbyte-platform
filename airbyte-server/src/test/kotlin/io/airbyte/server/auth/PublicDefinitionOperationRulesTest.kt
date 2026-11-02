/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.PublicDefinitionAuthorizationService
import io.airbyte.domain.services.auth.PublicDefinitionMetadata
import io.micronaut.security.authentication.Authentication
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.util.UUID

class PublicDefinitionOperationRulesTest {
  @Test
  fun `a granted declarative definition remains readable from a workspace other than its builder workspace`() {
    val workspace = UUID(0, 1)
    val definition = UUID(0, 2)
    val owner = WorkspaceAndOrganization(workspace, UUID(0, 3))
    val data = mockk<AuthorizationDataService>()
    every { data.workspaceAndOrganization(workspace) } returns owner
    every { data.requireActorDefinitionType(definition, true) } just Runs
    every { data.canUseActorDefinition(definition, owner) } returns true
    val definitions = mockk<PublicDefinitionAuthorizationService>()
    every { definitions.definition(definition, true) } returns PublicDefinitionMetadata(true, true)
    every { definitions.builderWorkspace(definition) } returns UUID(0, 4)
    val grants = mockk<PermissionService>()
    every { grants.getPermissionsByAuthUserId("caller") } returns
      listOf(Permission().withWorkspaceId(workspace).withPermissionType(Permission.PermissionType.WORKSPACE_READER))
    val permissions = OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true)
    val rule = PublicDeclarativeDefinitionReadRule(data, definitions, permissions)

    assertDoesNotThrow { rule.authorize(Authentication.build("caller"), rule.readInput(arrayOf(workspace, definition))) }
  }
}
