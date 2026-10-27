/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionStateCreateOrUpdate
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

/** The nested state connectionId is discarded by StateConverter, so it is not a second selector. */
data class ConnectionStateOperationInput(
  val connectionId: UUID,
) : OperationInput

abstract class ConnectionStateOperationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
  private val workersOnly: Boolean = false,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    ConnectionStateOperationInput(
      when (val body = args.singleOrNull()) {
        is ConnectionIdRequestBody -> body.connectionId
        is ConnectionStateCreateOrUpdate -> body.connectionId
        else -> throw BadRequestProblem()
      },
    )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers =
      if (workersOnly) {
        setOf(OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)
      } else {
        setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)
      }
    permissions.requireWorkspaces(caller, listOf(connectionOwner(data, (input as ConnectionStateOperationInput).connectionId)), roles, callers)
  }
}

@Singleton
class ConnectionStateReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionStateOperationRule(
    data,
    permissions,
    workspaceReadRoles + AuthRoleConstants.DATAPLANE,
  )

@Singleton
class ConnectionStateWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionStateOperationRule(data, permissions, connectionWriteRoles)

@Singleton
class ConnectionStateWorkerRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionStateOperationRule(data, permissions, setOf(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE), true)
