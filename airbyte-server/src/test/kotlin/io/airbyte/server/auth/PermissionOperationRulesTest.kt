/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.GroupPermissionIdRequestBody
import io.airbyte.api.server.generated.models.PermissionsCheckMultipleWorkspacesRequest
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.MembershipAuthorizationDataService
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class PermissionOperationRulesTest {
  @Test
  fun `diagnostic nullable targets remain explicit absent values in the immutable typed question`() {
    val body =
      MoreMappers.initMapper().readValue(
        """{"userId":"${UUID(0, 1)}","permissionType":"workspace_reader","workspaceIds":[null]}""",
        PermissionsCheckMultipleWorkspacesRequest::class.java,
      )
    val input = PermissionDiagnosticRule(mockk()).readInput(arrayOf(body)) as PermissionOperationInput.Diagnostic
    assertEquals(listOf(Optional.empty<UUID>()), input.workspaces)
  }

  @Test
  fun `group permission deletion consumes its saved group parent without active target workspace lookup`() {
    val group = UUID(0, 1)
    val permission = UUID(0, 2)
    val data = mockk<AuthorizationDataService>()
    val membership = mockk<MembershipAuthorizationDataService>()
    every { data.groupOrganization(group) } returns UUID(0, 3)
    every { membership.permissionGroup(permission) } returns Optional.of(group)
    val rule = GroupPermissionDeleteRule(data, OperationPermissions(mockk(), RoleResolver(mockk(), mockk(), mockk(), mockk()), false), membership)
    rule.authorize(Authentication.build("caller"), rule.readInput(arrayOf(GroupPermissionIdRequestBody(group, permission))))
    verify(exactly = 0) { membership.permission(any()) }
    verify(exactly = 0) { data.workspaceAndOrganization(any()) }
  }
}
