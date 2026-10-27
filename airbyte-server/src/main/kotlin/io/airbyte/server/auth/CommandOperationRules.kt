/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.CancelCommandRequest
import io.airbyte.api.server.generated.models.CheckCommandOutputRequest
import io.airbyte.api.server.generated.models.CommandGetRequest
import io.airbyte.api.server.generated.models.CommandStatusRequest
import io.airbyte.api.server.generated.models.DiscoverCommandOutputRequest
import io.airbyte.api.server.generated.models.ReplicateCommandOutputRequest
import io.airbyte.api.server.generated.models.RunDiscoverCommandRequest
import io.airbyte.api.server.generated.models.RunReplicateCommandRequest
import io.airbyte.api.server.generated.models.RunSpecCommandRequest
import io.airbyte.api.server.generated.models.SpecCommandOutputRequest
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SavedExecutionAuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

private val commandCallers = setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)

data class SavedCommandInput(
  val id: String,
) : OperationInput

abstract class SavedCommandRule(
  private val data: SavedExecutionAuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
  private val embedded: Boolean = false,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val id: String =
      when (val body = args.singleOrNull()) {
        is CancelCommandRequest -> body.id
        is CommandGetRequest -> body.id
        is CommandStatusRequest -> body.id
        is CheckCommandOutputRequest -> body.id
        is DiscoverCommandOutputRequest -> body.id
        is SpecCommandOutputRequest -> body.id
        is ReplicateCommandOutputRequest -> body.id
        else -> throw BadRequestProblem()
      }
    return SavedCommandInput(id.takeUnless(String::isBlank) ?: throw BadRequestProblem())
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val owner =
      try {
        data.command((input as SavedCommandInput).id)
      } catch (_: AuthorizationResourceNotFoundException) {
        throw ForbiddenProblem()
      }
    permissions.requireWorkspaces(
      caller,
      listOf(owner),
      roles,
      if (embedded) {
        commandCallers +
          OperationCaller.EMBEDDED
      } else {
        commandCallers
      },
    )
  }
}

@Singleton
class CancelCommandRule(
  data: SavedExecutionAuthorizationDataService,
  permissions: OperationPermissions,
) : SavedCommandRule(data, permissions, setOf(AuthRoleConstants.WORKSPACE_RUNNER))

@Singleton
class ReadCommandRule(
  data: SavedExecutionAuthorizationDataService,
  permissions: OperationPermissions,
) : SavedCommandRule(data, permissions, setOf(AuthRoleConstants.WORKSPACE_READER))

@Singleton
class ReadCheckCommandRule(
  data: SavedExecutionAuthorizationDataService,
  permissions: OperationPermissions,
) : SavedCommandRule(data, permissions, setOf(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.EMBEDDED_END_USER), true)

data class DiscoverCommandInput(
  val id: String,
  val actor: UUID,
  val job: Optional<String>,
  val attempt: Optional<Int>,
) : OperationInput

@Singleton
class RunDiscoverCommandRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? RunDiscoverCommandRequest ?: throw BadRequestProblem()
    if (body.id.isBlank()) throw BadRequestProblem()
    return DiscoverCommandInput(body.id, body.actorId, Optional.ofNullable(body.jobId), Optional.ofNullable(body.attemptNumber))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as DiscoverCommandInput
    val owner = data.actorWorkspaceAndOrganization(request.actor)
    permissions.requireWorkspaces(caller, listOf(owner), setOf(AuthRoleConstants.WORKSPACE_RUNNER), commandCallers)
    if (data.commandExistsInWorkspace(request.id, owner)) return
    if (request.job.isPresent != request.attempt.isPresent) throw BadRequestProblem()
  }
}

data class ReplicateCommandInput(
  val id: String,
  val connection: UUID,
  val job: String,
  val attempt: Int,
) : OperationInput

@Singleton
class RunReplicateCommandRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? RunReplicateCommandRequest ?: throw BadRequestProblem()
    if (body.id.isBlank()) throw BadRequestProblem()
    return ReplicateCommandInput(
      body.id,
      body.connectionId,
      body.jobId,
      body.attemptNumber,
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as ReplicateCommandInput
    val owner = data.connectionWorkspaceAndOrganization(request.connection)
    permissions.requireWorkspaces(caller, listOf(owner), setOf(AuthRoleConstants.WORKSPACE_RUNNER), commandCallers)
    if (data.commandExistsInWorkspace(request.id, owner)) return
    val job = request.job.toLongOrNull() ?: throw BadRequestProblem()
    if (data.savedAttemptConnectionId(job, request.attempt) != request.connection) throw BadRequestProblem()
  }
}

data class SpecCommandInput(
  val id: String,
  val workspace: UUID,
  val definition: Optional<UUID>,
) : OperationInput

@Singleton
class RunSpecCommandRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? RunSpecCommandRequest ?: throw BadRequestProblem()
    if (body.id.isBlank()) throw BadRequestProblem()
    return SpecCommandInput(body.id, body.workspaceId, Optional.ofNullable(body.actorDefinitionId))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as SpecCommandInput
    val owner = data.workspaceAndOrganization(request.workspace)
    permissions.requireWorkspaces(caller, listOf(owner), setOf(AuthRoleConstants.WORKSPACE_RUNNER), commandCallers)
    if (data.commandExistsInWorkspace(request.id, owner)) return
    request.definition.ifPresent { if (!data.canUseActorDefinition(it, owner)) throw ForbiddenProblem() }
  }
}
