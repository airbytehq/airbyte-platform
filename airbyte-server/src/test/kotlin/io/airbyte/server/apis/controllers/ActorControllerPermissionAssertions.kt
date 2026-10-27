/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SavedConnectionReferences
import io.airbyte.server.auth.ActorOperationInput
import io.airbyte.server.auth.ConnectionOperationInput
import io.airbyte.server.auth.ConnectionOperationRule
import io.airbyte.server.auth.OperationPermissions
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import java.util.Optional
import java.util.UUID

internal fun assertActorControllerPermissions(
  controller: Class<*>,
  endpoints: Set<String>,
  allowed: Permission.PermissionType,
  denied: Permission.PermissionType,
) {
  val methods = controller.declaredMethods.filter { it.name in endpoints }
  assertEquals(endpoints, methods.map { it.name }.toSet())
  val actor = UUID(0, 901)
  val owner = WorkspaceAndOrganization(UUID(0, 902), UUID(0, 903))
  val data = mockk<AuthorizationDataService>()
  every { data.savedConnectionActorIds(actor) } returns (actor to actor)
  every { data.savedConnectionReferences(actor) } returns
    SavedConnectionReferences(actor, actor, Optional.empty(), Optional.empty(), emptyList())
  every { data.actorConnectionIds(any(), any(), any()) } returns emptyList()
  every { data.savedSourceWorkspaceAndOrganization(actor) } returns owner
  every { data.savedDestinationWorkspaceAndOrganization(actor) } returns owner
  val saved = mockk<PermissionService>()
  val permissions = OperationPermissions(saved, RoleResolver(mockk(), mockk(), mockk(), mockk()), true)
  val caller =
    io.micronaut.security.authentication.Authentication
      .build("caller")
  for (method in methods) {
    val rule =
      method
        .getAnnotation(AuthorizeOperation::class.java)
        .rule.java
        .getConstructor(AuthorizationDataService::class.java, OperationPermissions::class.java)
        .newInstance(data, permissions)
    every { saved.getPermissionsByAuthUserId("caller") } returns listOf(Permission().withWorkspaceId(owner.workspaceId).withPermissionType(allowed))
    val input = if (rule is ConnectionOperationRule) ConnectionOperationInput.Saved(listOf(actor)) else ActorOperationInput.Saved(actor)
    assertDoesNotThrow { rule.authorize(caller, input) }
    val deniedGrant = Permission().withPermissionType(denied)
    if (denied.name.startsWith("ORGANIZATION")) deniedGrant.organizationId = owner.organizationId else deniedGrant.workspaceId = owner.workspaceId
    every { saved.getPermissionsByAuthUserId("caller") } returns listOf(deniedGrant)
    assertThrows(ForbiddenProblem::class.java) { rule.authorize(caller, input) }
  }
}
