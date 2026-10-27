/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.server.generated.models.ActorDefinitionCreateRequest
import io.airbyte.api.server.generated.models.ActorDefinitionFinishRequest
import io.airbyte.api.server.generated.models.ActorDefinitionResultRequest
import io.airbyte.api.server.generated.models.ActorDefinitionUpdateRequest
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SavedExecutionAuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

val actorDefinitionCallers = setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)

data class ActorDefinitionCreateInput(
  val workspace: UUID,
) : OperationInput

@Singleton
class ActorDefinitionCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ActorDefinitionCreateRequest ?: throw BadRequestProblem()
    return ActorDefinitionCreateInput(body.workspaceId ?: throw BadRequestProblem())
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as ActorDefinitionCreateInput
    permissions.requireWorkspaces(
      caller,
      listOf(data.workspaceAndOrganization(request.workspace)),
      Intent.UploadCustomConnector.roles,
      actorDefinitionCallers,
    )
  }
}

data class ActorDefinitionFinishInput(
  val workspace: UUID,
  val definition: Optional<UUID>,
  val source: Boolean,
  val command: Optional<String>,
) : OperationInput

@Singleton
open class ActorDefinitionFinishRule(
  protected val data: AuthorizationDataService,
  protected val permissions: OperationPermissions,
  private val executions: SavedExecutionAuthorizationDataService,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ActorDefinitionFinishRequest ?: throw BadRequestProblem()
    return ActorDefinitionFinishInput(
      body.workspaceId,
      Optional.ofNullable(body.actorUpdateRequest.actorDefinitionId),
      body.actorUpdateRequest.actorType.toString() == "source",
      Optional.ofNullable(body.commandId),
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as ActorDefinitionFinishInput
    val owner = data.workspaceAndOrganization(request.workspace)
    permissions.requireWorkspaces(caller, listOf(owner), Intent.UploadCustomConnector.roles, actorDefinitionCallers)
    request.definition.ifPresent { data.requireActorDefinitionType(it, request.source) }
    request.command.ifPresent { if (executions.specCommand(it) != owner) throw BadRequestProblem() }
  }
}

data class ActorDefinitionResultInput(
  val request: String,
) : OperationInput

@Singleton
class ActorDefinitionResultRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val temporal: TemporalClient,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ActorDefinitionResultRequest ?: throw BadRequestProblem()
    if (body.requestId.isBlank()) throw BadRequestProblem()
    return ActorDefinitionResultInput(body.requestId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val saved =
      try {
        temporal.getActorDefinitionWorkflowOwner((input as ActorDefinitionResultInput).request)
      } catch (_: NoSuchElementException) {
        throw ResourceNotFoundProblem()
      }
    val owner = data.workspaceAndOrganization(saved.workspaceId)
    permissions.requireWorkspaces(caller, listOf(owner), setOf(AuthRoleConstants.WORKSPACE_READER), actorDefinitionCallers)
    saved.actorDefinitionId.ifPresent { data.requireActorDefinitionType(it, saved.actorType == io.airbyte.config.ActorType.SOURCE) }
  }
}

data class ActorDefinitionUpdateInput(
  val workspace: UUID,
  val definition: UUID,
  val source: Boolean,
) : OperationInput

@Singleton
open class ActorDefinitionUpdateRule(
  protected val data: AuthorizationDataService,
  protected val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ActorDefinitionUpdateRequest ?: throw BadRequestProblem()
    return ActorDefinitionUpdateInput(body.workspaceId, body.actorDefinitionId, body.actorType.toString() == "source")
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as ActorDefinitionUpdateInput
    permissions.requireAuthenticated(
      caller,
      actorDefinitionCallers + OperationCaller.EMBEDDED,
      listOf(data.workspaceAndOrganization(request.workspace)),
    )
    data.requireActorDefinitionType(request.definition, request.source)
    try {
      data.requireDefinitionDefaultVersion(request.definition)
    } catch (_: NoSuchElementException) {
      throw ResourceNotFoundProblem()
    }
  }
}
