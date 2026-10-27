/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.ActorType
import io.airbyte.api.server.generated.models.ConnectorDocumentationRequestBody
import io.airbyte.api.server.generated.models.DiagnosticReportRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class ConnectorDocumentationInput(
  val workspace: UUID,
  val definition: UUID,
  val source: Boolean,
  val actor: Optional<UUID>,
) : OperationInput

@Singleton
class ConnectorDocumentationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ConnectorDocumentationRequestBody ?: throw BadRequestProblem()
    return ConnectorDocumentationInput(
      body.workspaceId,
      body.actorDefinitionId,
      body.actorType == ActorType.SOURCE,
      Optional.ofNullable(body.actorId),
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as ConnectorDocumentationInput
    val owner = data.workspaceAndOrganization(request.workspace)
    permissions.requireWorkspaces(caller, listOf(owner), workspaceReadRoles, organizationCallers)
    data.requireActorDefinitionType(request.definition, request.source)
    if (!data.canUseActorDefinition(request.definition, owner)) throw ForbiddenProblem()
    request.actor.ifPresent { actor ->
      val actorOwner = if (request.source) data.savedSourceWorkspaceAndOrganization(actor) else data.savedDestinationWorkspaceAndOrganization(actor)
      if (actorOwner != owner || data.savedActorDefinitionId(actor, owner.workspaceId) != request.definition) throw BadRequestProblem()
    }
  }
}

/** The report includes every workspace and instance infrastructure, regardless of the unused body. */
@Singleton
class DiagnosticReportRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    if (args.singleOrNull() !is DiagnosticReportRequestBody) throw BadRequestProblem()
    return MetadataOperationInput
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
  }
}
