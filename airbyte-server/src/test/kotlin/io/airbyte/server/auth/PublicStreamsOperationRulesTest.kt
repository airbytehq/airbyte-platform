/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.util.UUID

class PublicStreamsOperationRulesTest {
  @Test
  fun `instance administrators and no auth retain cross workspace stream schema reads`() {
    val source = UUID(0, 1)
    val destination = UUID(0, 2)
    val data = mockk<AuthorizationDataService>()
    every { data.savedSourceWorkspaceAndOrganization(source) } returns WorkspaceAndOrganization(UUID(0, 3), UUID(0, 4))
    every { data.savedDestinationWorkspaceAndOrganization(destination) } returns WorkspaceAndOrganization(UUID(0, 5), UUID(0, 6))
    val grants = mockk<PermissionService>()
    every { grants.getPermissionsByAuthUserId("caller") } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.INSTANCE_ADMIN))
    for (security in listOf(true, false)) {
      val permissions = OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security)
      val rule = PublicStreamsRule(data, permissions)
      assertDoesNotThrow {
        rule.authorize(Authentication.build("caller"), rule.readInput(arrayOf(source.toString(), destination.toString(), false)))
      }
    }
  }
}
