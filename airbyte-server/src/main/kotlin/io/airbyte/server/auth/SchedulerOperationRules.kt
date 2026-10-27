/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.DestinationCoreConfig
import io.airbyte.api.server.generated.models.SourceCoreConfig
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class SchedulerOperationInput(
  val workspaceId: UUID,
  val definitionId: UUID,
  val actor: Optional<UUID>,
) : OperationInput

/** The optional actor selects a saved connector-version override, even for an inline configuration. */
abstract class SchedulerOperationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is SourceCoreConfig -> SchedulerOperationInput(body.workspaceId, body.sourceDefinitionId, Optional.ofNullable(body.sourceId))
      is DestinationCoreConfig -> SchedulerOperationInput(body.workspaceId, body.destinationDefinitionId, Optional.ofNullable(body.destinationId))
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as SchedulerOperationInput
    val owner = data.workspaceAndOrganization(request.workspaceId)
    permissions.requireWorkspaces(
      caller,
      listOf(owner),
      if (source) sourceWriteRoles else destinationWriteRoles,
      setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT),
    )
    data.requireActorDefinitionType(request.definitionId, source)
    if (!data.canUseActorDefinition(request.definitionId, owner)) throw ForbiddenProblem()
    request.actor.ifPresent { actor ->
      val actorOwner = if (source) data.savedSourceWorkspaceAndOrganization(actor) else data.savedDestinationWorkspaceAndOrganization(actor)
      if (actorOwner != owner) throw BadRequestProblem()
      if (data.savedActorDefinitionId(actor, owner.workspaceId) != request.definitionId) throw BadRequestProblem()
    }
  }
}

@Singleton
class SchedulerSourceRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : SchedulerOperationRule(data, permissions, true)

@Singleton
class SchedulerDestinationRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : SchedulerOperationRule(data, permissions, false)
