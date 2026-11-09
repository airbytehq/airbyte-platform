/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.ConnectionSearch
import io.airbyte.api.server.generated.models.DestinationSearch
import io.airbyte.api.server.generated.models.SourceSearch
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton

data class ConfigSearchInput(
  val references: Map<String, String>,
) : OperationInput

/** Keeps Config root searches authenticated while enforcing workspace scope on embedded tokens. */
@Singleton
class ConfigSearchRule(
  private val scopeResolver: AuthenticationHeaderResolver,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val references =
      buildMap {
        when (val body = args.singleOrNull()) {
          is SourceSearch -> {
            body.workspaceId?.let { put(AuthenticationId.WORKSPACE_ID.httpHeader, it.toString()) }
            body.sourceId?.let { put(AuthenticationId.SOURCE_ID.httpHeader, it.toString()) }
          }
          is DestinationSearch -> {
            body.workspaceId?.let { put(AuthenticationId.WORKSPACE_ID.httpHeader, it.toString()) }
            body.destinationId?.let { put(AuthenticationId.DESTINATION_ID_.httpHeader, it.toString()) }
          }
          is ConnectionSearch -> {
            body.connectionId?.let { put(AuthenticationId.CONNECTION_ID.httpHeader, it.toString()) }
            body.sourceId?.let { put(AuthenticationId.SOURCE_ID.httpHeader, it.toString()) }
            body.destinationId?.let { put(AuthenticationId.DESTINATION_ID_.httpHeader, it.toString()) }
          }
          null -> Unit
          else -> throw BadRequestProblem()
        }
      }
    return ConfigSearchInput(references)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val scope = getTokenScopeClaim(caller) ?: return
    val requestedWorkspace = scopeResolver.resolveWorkspace((input as ConfigSearchInput).references)?.singleOrNull()
    if (requestedWorkspace?.toString() != scope.workspaceId) throw ForbiddenProblem()
  }
}
