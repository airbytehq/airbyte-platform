/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaWriteRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

data class WriteDiscoverCatalogResultInput(
  val sourceId: UUID,
) : OperationInput

@Singleton
class WriteDiscoverCatalogResultRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WriteDiscoverCatalogResultInput {
    val body = args.singleOrNull() as? SourceDiscoverSchemaWriteRequestBody ?: throw BadRequestProblem()
    return WriteDiscoverCatalogResultInput(body.sourceId ?: throw BadRequestProblem())
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val owner = data.savedSourceWorkspaceAndOrganization((input as WriteDiscoverCatalogResultInput).sourceId)
    permissions.requireWorkspaces(
      caller,
      listOf(owner),
      setOf(AuthRoleConstants.DATAPLANE),
      // Internal clients retain their existing instance-admin privilege; accounts use saved grants.
      setOf(OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT),
    )
  }
}
