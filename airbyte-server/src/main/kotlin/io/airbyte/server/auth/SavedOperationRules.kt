/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.OperationCreate
import io.airbyte.api.server.generated.models.OperationIdRequestBody
import io.airbyte.api.server.generated.models.OperationUpdate
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

sealed interface SavedOperationInput : OperationInput {
  data class Create(
    val workspaceId: UUID,
  ) : SavedOperationInput

  data class Saved(
    val operationId: UUID,
  ) : SavedOperationInput

  data class Connection(
    val connectionId: UUID,
  ) : SavedOperationInput
}

private fun savedOperationInput(args: Array<out Any?>): SavedOperationInput =
  when (val body = args.singleOrNull()) {
    is OperationCreate -> SavedOperationInput.Create(body.workspaceId)
    is OperationUpdate -> SavedOperationInput.Saved(body.operationId)
    is OperationIdRequestBody -> SavedOperationInput.Saved(body.operationId)
    is ConnectionIdRequestBody -> SavedOperationInput.Connection(body.connectionId)
    else -> throw BadRequestProblem()
  }

abstract class SavedOperationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = savedOperationInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)
    when (val request = input as SavedOperationInput) {
      is SavedOperationInput.Create ->
        permissions.requireWorkspaces(
          caller,
          listOf(data.workspaceAndOrganization(request.workspaceId)),
          roles,
          callers,
        )
      is SavedOperationInput.Saved ->
        permissions.requireWorkspaces(
          caller,
          listOf(data.operationWorkspaceAndOrganization(request.operationId)),
          roles,
          callers,
        )
      is SavedOperationInput.Connection -> {
        val owner = connectionOwner(data, request.connectionId)
        permissions.requireWorkspaces(caller, listOf(owner), roles, callers)
        for (operationId in data.savedConnectionOperationIds(request.connectionId)) {
          if (data.operationWorkspaceAndOrganization(operationId) != owner) throw BadRequestProblem()
        }
      }
    }
  }
}

@Singleton
class SavedOperationReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : SavedOperationRule(data, permissions, workspaceReadRoles)

@Singleton
class SavedOperationWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : SavedOperationRule(data, permissions, connectionWriteRoles)

@Singleton
class SavedOperationListRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : SavedOperationRule(data, permissions, workspaceReadRoles)
