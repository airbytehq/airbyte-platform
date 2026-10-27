/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.ActorType
import io.airbyte.api.server.generated.models.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.GetActorDefinitionVersionDefaultRequestBody
import io.airbyte.api.server.generated.models.ResolveActorDefinitionVersionRequestBody
import io.airbyte.api.server.generated.models.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

sealed interface DefinitionMetadataInput : OperationInput {
  data class Actor(
    val actorId: UUID,
  ) : DefinitionMetadataInput

  data class WorkspaceDefinition(
    val workspaceId: UUID,
    val definitionId: UUID,
  ) : DefinitionMetadataInput

  data class GlobalDefault(
    val definitionId: UUID,
  ) : DefinitionMetadataInput

  data class Resolve(
    val definitionId: UUID,
    val source: Boolean,
  ) : DefinitionMetadataInput
}

private fun definitionMetadataInput(args: Array<out Any?>): DefinitionMetadataInput =
  when (val body = args.singleOrNull()) {
    is SourceIdRequestBody -> DefinitionMetadataInput.Actor(body.sourceId)
    is DestinationIdRequestBody -> DefinitionMetadataInput.Actor(body.destinationId)
    is SourceDefinitionIdWithWorkspaceId -> DefinitionMetadataInput.WorkspaceDefinition(body.workspaceId, body.sourceDefinitionId)
    is DestinationDefinitionIdWithWorkspaceId -> DefinitionMetadataInput.WorkspaceDefinition(body.workspaceId, body.destinationDefinitionId)
    is GetActorDefinitionVersionDefaultRequestBody -> DefinitionMetadataInput.GlobalDefault(body.actorDefinitionId)
    is ResolveActorDefinitionVersionRequestBody -> DefinitionMetadataInput.Resolve(body.actorDefinitionId, body.actorType == ActorType.SOURCE)
    else -> throw BadRequestProblem()
  }

private val definitionMetadataCallers =
  setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)

/** These specification/version reads retain authentication-only access to the typed saved resources. */
abstract class DefinitionActorMetadataRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = definitionMetadataInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val (owner, definition) =
      when (val request = input as DefinitionMetadataInput) {
        is DefinitionMetadataInput.Actor -> {
          val owner =
            if (source) {
              data.savedSourceWorkspaceAndOrganization(
                request.actorId,
              )
            } else {
              data.savedDestinationWorkspaceAndOrganization(request.actorId)
            }
          owner to data.savedActorDefinitionId(request.actorId, owner.workspaceId)
        }
        is DefinitionMetadataInput.WorkspaceDefinition -> data.workspaceAndOrganization(request.workspaceId) to request.definitionId
        else -> throw BadRequestProblem()
      }
    permissions.requireAuthenticated(caller, definitionMetadataCallers, listOf(owner))
    data.requireActorDefinitionType(definition, source)
    if (!data.canUseActorDefinition(definition, owner)) throw ForbiddenProblem()
  }
}

@Singleton
class DefinitionSourceMetadataRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionActorMetadataRule(data, permissions, true)

@Singleton
class DefinitionDestinationMetadataRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionActorMetadataRule(data, permissions, false)

@Singleton
class DefinitionDefaultVersionRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = definitionMetadataInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), definitionMetadataCallers)
    data.requireDefinitionDefaultVersion((input as DefinitionMetadataInput.GlobalDefault).definitionId)
  }
}

@Singleton
class DefinitionResolveVersionRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = definitionMetadataInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE), definitionMetadataCallers)
    val request = input as DefinitionMetadataInput.Resolve
    data.requireActorDefinitionType(request.definitionId, request.source)
    data.requireDefinitionDefaultVersion(request.definitionId)
  }
}
