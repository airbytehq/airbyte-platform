/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.GetEntitlementsByOrganizationIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.OrganizationAccessService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

data class OrganizationOperationInput(
  val organizationId: UUID,
) : OperationInput

@Singleton
class OrganizationEntitlementsRule(
  data: AuthorizationDataService,
  access: OrganizationAccessService,
  permissions: OperationPermissions,
) : OrganizationAccessibleOperationRule(data, access, permissions) {
  override fun readInput(args: Array<out Any?>): OrganizationOperationInput {
    val body = args.singleOrNull() as? GetEntitlementsByOrganizationIdRequestBody ?: throw BadRequestProblem()
    return OrganizationOperationInput(body.organizationId)
  }
}

abstract class OrganizationAccessibleOperationRule(
  private val data: AuthorizationDataService,
  private val access: OrganizationAccessService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val organization = data.organization((input as OrganizationOperationInput).organizationId)
    val callers = setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)
    permissions.requireAuthenticated(caller, callers)
    try {
      permissions.requireOrganizations(caller, listOf(organization), setOf(AuthRoleConstants.ORGANIZATION_MEMBER), callers)
    } catch (denied: ForbiddenProblem) {
      permissions.requireAuthenticated(caller, setOf(OperationCaller.USER))
      if (!access.hasAccessibleWorkspace(caller.name, organization)) throw denied
    }
  }
}
