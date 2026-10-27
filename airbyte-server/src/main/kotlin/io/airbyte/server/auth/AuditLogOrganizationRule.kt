/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.AuditLogListRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class AuditLogOrganizationInput(
  val organizationId: UUID,
  val workspaceFilter: Optional<UUID>,
) : OperationInput

@Singleton
class AuditLogOrganizationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): AuditLogOrganizationInput {
    val body = args.singleOrNull() as? AuditLogListRequestBody ?: throw BadRequestProblem()
    return AuditLogOrganizationInput(body.organizationId, Optional.ofNullable(body.workspaceId))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val organization = data.organization((input as AuditLogOrganizationInput).organizationId)
    permissions.requireOrganizations(caller, listOf(organization), setOf(AuthRoleConstants.ORGANIZATION_ADMIN), organizationCallers)
  }
}
