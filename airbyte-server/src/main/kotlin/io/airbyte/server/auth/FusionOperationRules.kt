/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.AbstractThrowableProblem
import io.airbyte.api.problems.model.generated.ServiceUnavailableProblemResponse
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.FusionEnablementResolveRequest
import io.airbyte.api.server.generated.models.FusionSearchRequest
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Optional
import java.util.UUID

data class FusionSearchInput(
  val actorId: UUID,
  val destinationId: Optional<UUID>,
  val sourceIds: List<UUID>,
) : OperationInput

abstract class FusionSearchRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): FusionSearchInput {
    val actorId = args.firstOrNull() as? UUID ?: throw BadRequestProblem()
    val body = args.getOrNull(1) as? FusionSearchRequest ?: throw BadRequestProblem()
    val sourceIds =
      body.streams
        .orEmpty()
        .map { stream ->
          val id: UUID? = stream.sourceId
          id
            ?: if (source) {
              actorId
            } else {
              throw FusionSearchAuthorizationProblem(
                "source_id_required",
                "streams[].source_id is required when searching a destination or workspace",
              )
            }
        }.toList()
    return FusionSearchInput(actorId, Optional.ofNullable(body.destinationId), sourceIds)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as FusionSearchInput
    val owner =
      fusionResourceRead {
        if (source) {
          data.savedSourceWorkspaceAndOrganization(
            request.actorId,
          )
        } else {
          data.savedDestinationWorkspaceAndOrganization(request.actorId)
        }
      }
    try {
      permissions.requireWorkspaces(caller, listOf(owner), setOf(AuthRoleConstants.WORKSPACE_READER), OperationCaller.entries.toSet())
    } catch (_: ForbiddenProblem) {
      throw FusionSearchAuthorizationProblem()
    }
    request.destinationId.ifPresent { destinationId ->
      if (!source &&
        destinationId != request.actorId
      ) {
        throw FusionSearchAuthorizationProblem("destination_id_mismatch", "destination_id must match the searched destination")
      }
      if (source &&
        fusionResourceRead { data.savedDestinationWorkspaceAndOrganization(destinationId) } != owner
      ) {
        throw FusionSearchAuthorizationProblem()
      }
    }
    for (sourceId in request.sourceIds) {
      if (source &&
        sourceId != request.actorId
      ) {
        throw FusionSearchAuthorizationProblem("source_id_mismatch", "streams[].source_id must match the searched source")
      }
      if (!source && fusionResourceRead { data.savedSourceWorkspaceAndOrganization(sourceId) } != owner) throw FusionSearchAuthorizationProblem()
    }
  }
}

/** Preserve Fusion's non-disclosing 404 and stable selector error contract. */
private class FusionSearchAuthorizationProblem(
  code: String = "",
  message: String = "",
  statusCode: Int = if (code.isEmpty()) 404 else 422,
) : AbstractThrowableProblem(
    ServiceUnavailableProblemResponse()
      .status(statusCode)
      .title(
        when (statusCode) {
          404 -> "not-found"
          500 -> "internal-error"
          else -> "invalid-input"
        },
      ).detail(message.ifEmpty { if (statusCode == 500) "The request failed." else "The resource was not found." })
      .data(if (code.isEmpty()) null else mapOf("error_code" to code)),
  )

@Singleton
class FusionSourceSearchRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : FusionSearchRule(data, permissions, true)

@Singleton
class FusionDestinationSearchRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : FusionSearchRule(data, permissions, false)

data class FusionResolveInput(
  val organizationId: UUID,
  val workspaceId: UUID,
  val connectionId: UUID,
  val sourceId: UUID,
  val destinationId: UUID,
  val dataplaneGroupId: UUID,
  val workloadId: String,
) : OperationInput

@Singleton
class FusionResolveRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): FusionResolveInput {
    val body = args.singleOrNull() as? FusionEnablementResolveRequest ?: throw BadRequestProblem()
    return FusionResolveInput(
      body.organizationId,
      body.workspaceId,
      body.connectionId,
      body.sourceId,
      body.destinationId,
      body.dataplaneGroupId,
      body.workloadId.takeIf { it.isNotBlank() } ?: throw BadRequestProblem(),
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as FusionResolveInput
    val references = data.savedConnectionReferences(request.connectionId)
    if (references.sourceId != request.sourceId || references.destinationId != request.destinationId) throw ForbiddenProblem()
    val owner = connectionOwner(data, references)
    if (owner.workspaceId != request.workspaceId || owner.organizationId != request.organizationId) throw ForbiddenProblem()
    if (!data.canUseDataplaneGroup(request.dataplaneGroupId, owner.organizationId)) throw ForbiddenProblem()
    permissions.requireWorkspaces(caller, listOf(owner), setOf(AuthRoleConstants.DATAPLANE), OperationCaller.entries.toSet())
    val accountId =
      try {
        UUID.fromString(caller.name)
      } catch (_: IllegalArgumentException) {
        throw ForbiddenProblem()
      }
    if (!data.isAssignedSyncWorkload(
        request.organizationId,
        request.workspaceId,
        request.connectionId,
        request.sourceId,
        request.destinationId,
        request.workloadId,
        request.dataplaneGroupId,
        accountId,
      )
    ) {
      throw ForbiddenProblem()
    }
    // The Cloud resolver retains its signed payload-identity checks.
  }
}

abstract class FusionSearchStatusRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): ActorOperationInput =
    ActorOperationInput.Saved(args.singleOrNull() as? UUID ?: throw BadRequestProblem())

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val id = (input as ActorOperationInput.Saved).actorId
    val owner = fusionResourceRead { if (source) data.savedSourceWorkspaceAndOrganization(id) else data.savedDestinationWorkspaceAndOrganization(id) }
    try {
      permissions.requireWorkspaces(caller, listOf(owner), setOf(AuthRoleConstants.WORKSPACE_READER), OperationCaller.entries.toSet())
    } catch (_: ForbiddenProblem) {
      throw FusionSearchAuthorizationProblem()
    }
  }
}

@Singleton
class FusionSourceSearchStatusRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : FusionSearchStatusRule(data, permissions, true)

@Singleton
class FusionDestinationSearchStatusRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : FusionSearchStatusRule(data, permissions, false)

private fun <T> fusionResourceRead(read: () -> T): T =
  try {
    read()
  } catch (_: AuthorizationResourceNotFoundException) {
    throw FusionSearchAuthorizationProblem()
  } catch (_: IOException) {
    throw FusionSearchAuthorizationProblem(statusCode = 500)
  }

private fun executionActorId(args: Array<out Any?>): UUID =
  when (val value = args.firstOrNull()) {
    is UUID -> value
    is String ->
      try {
        UUID.fromString(value)
      } catch (_: IllegalArgumentException) {
        throw BadRequestProblem()
      }
    else -> throw BadRequestProblem()
  }

@Singleton
class FusionSourceExecuteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(data, permissions, true, setOf(AuthRoleConstants.WORKSPACE_READER)) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = ActorOperationInput.Saved(executionActorId(args))

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = fusionResourceRead { super.authorize(caller, input) }
}

@Singleton
class FusionDestinationExecuteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(data, permissions, false, setOf(AuthRoleConstants.WORKSPACE_READER)) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = ActorOperationInput.Saved(executionActorId(args))

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = fusionResourceRead { super.authorize(caller, input) }
}
