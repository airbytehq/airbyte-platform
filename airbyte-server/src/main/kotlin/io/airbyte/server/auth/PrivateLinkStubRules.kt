/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.PrivateLinkCreateRequestBody
import io.airbyte.api.server.generated.models.PrivateLinkIdRequestBody
import io.airbyte.api.server.generated.models.PrivateLinkListRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class PrivateLinkWorkspaceStubInput(
  val workspace: Optional<UUID>,
  val edit: Boolean,
) : OperationInput

@Singleton
class PrivateLinkWorkspaceStubRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is PrivateLinkCreateRequestBody -> PrivateLinkWorkspaceStubInput(Optional.ofNullable(body.workspaceId), true)
      is PrivateLinkListRequestBody -> PrivateLinkWorkspaceStubInput(Optional.ofNullable(body.workspaceId), false)
      is PrivateLinkIdRequestBody -> PrivateLinkWorkspaceStubInput(Optional.ofNullable(body.workspaceId), true)
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as PrivateLinkWorkspaceStubInput
    val roles = if (request.edit) connectionWriteRoles else workspaceReadRoles
    try {
      permissions.requireGlobal(caller, roles, organizationCallers)
      return
    } catch (_: ForbiddenProblem) {
      // Workspace grants require only the saved workspace, never the unconsumed private-link ID.
    }
    val workspace = request.workspace.orElseThrow { ForbiddenProblem() }
    permissions.requireWorkspaces(
      caller,
      listOf(data.workspaceAndOrganization(workspace)),
      roles,
      organizationCallers,
    )
  }
}

@Singleton
class PrivateLinkGlobalStubRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = MetadataOperationInput

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
}
