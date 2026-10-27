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
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import java.util.UUID

/** Existing controller tests exercise their moved authorization through the real shared rule. */
fun requireGroupAdminForControllerTest(
  group: UUID,
  organization: UUID,
  body: Any,
) {
  val data = mockk<AuthorizationDataService>()
  every { data.groupOrganization(group) } returns organization
  val grants = mockk<PermissionService>()
  every { grants.getPermissionsByAuthUserId("caller") } returns
    listOf(Permission().withOrganizationId(UUID.randomUUID()).withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN))
  val permissions = OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true)
  val rule = GroupSavedAdminRule(data, permissions)
  rule.authorize(Authentication.build("caller"), rule.readInput(arrayOf(body)))
}

fun requireCommunityDefinitionForControllerTest(
  source: Boolean,
  body: Any,
) {
  val data = mockk<AuthorizationDataService>()
  val grants = mockk<PermissionService>()
  every { grants.getPermissionsByAuthUserId("caller") } returns emptyList()
  val permissions = OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true)
  val rule = if (source) SourceDefinitionMutationRule(data, permissions) else DestinationDefinitionMutationRule(data, permissions)
  val input = rule.readInput(arrayOf(body)) as DefinitionMutationInput
  every { data.requireActorDefinitionType(input.definition, source) } just Runs
  if (input is DefinitionMutationInput.Update) {
    val owner = WorkspaceAndOrganization(input.workspace, UUID(0, 2))
    every { data.workspaceAndOrganization(input.workspace) } returns owner
    every { data.requireDefinitionDefaultVersion(input.definition) } just Runs
    every { data.canUseActorDefinition(input.definition, owner) } returns true
  }
  rule.authorize(Authentication.build("caller"), input)
}
