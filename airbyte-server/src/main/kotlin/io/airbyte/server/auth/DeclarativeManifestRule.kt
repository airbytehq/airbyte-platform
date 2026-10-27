/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.DeclarativeSourceDefinitionCreateManifestRequestBody
import io.airbyte.api.server.generated.models.ListDeclarativeManifestsRequestBody
import io.airbyte.api.server.generated.models.UpdateActiveManifestRequestBody
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.errors.DeclarativeSourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.DeclarativeManifestAuthorizationService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class DeclarativeManifestInput(
  val workspace: UUID,
  val definition: UUID,
  val version: Optional<Long>,
) : OperationInput

@Singleton
class DeclarativeManifestRule(
  private val data: AuthorizationDataService,
  private val manifests: DeclarativeManifestAuthorizationService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is DeclarativeSourceDefinitionCreateManifestRequestBody ->
        DeclarativeManifestInput(
          body.workspaceId,
          body.sourceDefinitionId,
          Optional.of(body.declarativeManifest.version),
        )
      is UpdateActiveManifestRequestBody -> DeclarativeManifestInput(body.workspaceId, body.sourceDefinitionId, Optional.of(body.version))
      is ListDeclarativeManifestsRequestBody -> DeclarativeManifestInput(body.workspaceId, body.sourceDefinitionId, Optional.empty())
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as DeclarativeManifestInput
    val owner = data.workspaceAndOrganization(request.workspace)
    permissions.requireWorkspaces(caller, listOf(owner), sourceWriteRoles, organizationCallers)
    if (!manifests.canUseSource(request.definition, request.workspace)) {
      throw DeclarativeSourceNotFoundException("Can't find source definition id `${request.definition}` in workspace ${request.workspace}")
    }
  }
}
