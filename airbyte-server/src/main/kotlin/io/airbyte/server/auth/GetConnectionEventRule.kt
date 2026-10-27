/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.api.server.generated.models.ConnectionEventIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

data class GetConnectionEventInput(
  val eventId: UUID,
  val connectionId: UUID,
) : OperationInput

@Singleton
class GetConnectionEventRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): GetConnectionEventInput {
    val body = args.singleOrNull() as? ConnectionEventIdRequestBody ?: throw BadRequestProblem()
    return GetConnectionEventInput(body.connectionEventId, body.connectionId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as GetConnectionEventInput
    val savedConnectionId = data.connectionEventConnectionId(request.eventId)
    val owner = data.connectionWorkspaceAndOrganization(savedConnectionId)
    permissions.requireWorkspaces(
      caller,
      listOf(owner),
      setOf(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER),
      OperationCaller.entries.toSet(),
    )

    if (request.connectionId != savedConnectionId) throw UnprocessableEntityProblem("Event does not belong to the requested connection.", null)
  }
}
