/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.publicApi.server.generated.models.DataplaneCreateRequest
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class PublicDataplaneCreateRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.REGION, setOf(AuthRoleConstants.ORGANIZATION_ADMIN)) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    PublicResourceInput(
      (
        args.firstOrNull() as? DataplaneCreateRequest
          ?: throw BadRequestProblem()
      ).regionId,
    )
}

data class PublicDataplaneListInput(
  val regions: List<UUID>,
) : OperationInput

@Singleton
class PublicDataplaneListRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    @Suppress("UNCHECKED_CAST")
    val regions = copyIds(args.firstOrNull() as? List<UUID?>)
    return PublicDataplaneListInput(regions)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    permissions.requireCaller(caller, callers)
    val regions = (input as PublicDataplaneListInput).regions
    if (regions.isEmpty()) {
      // The service filters the empty-selector request to the current user's administered organizations.
      permissions.requireAuthenticated(caller, callers)
      return
    }
    permissions.requireOrganizations(caller, regions.map(data::dataplaneGroupOrganization), setOf(AuthRoleConstants.ORGANIZATION_ADMIN), callers)
  }
}

@Singleton
class PublicConnectorDefinitionListRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.WORKSPACE, setOf(AuthRoleConstants.WORKSPACE_READER)) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    PublicResourceInput(args.getOrNull(1) as? UUID ?: throw IllegalArgumentException("Workspace ID must be provided."))
}
