/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest
import io.airbyte.publicApi.server.generated.models.JobCreateRequest
import io.airbyte.publicApi.server.generated.models.JobTypeEnum
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class PublicStreamsInput(
  val source: UUID,
  val destination: Optional<UUID>,
) : OperationInput

@Singleton
class PublicStreamsRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    PublicStreamsInput(publicResourceId(args.firstOrNull()), Optional.ofNullable(args.getOrNull(1)).map(::publicResourceId))

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    permissions.requireCaller(caller, callers)
    val request = input as PublicStreamsInput
    val owners =
      listOf(data.savedSourceWorkspaceAndOrganization(request.source)) +
        request.destination.map { listOf(data.savedDestinationWorkspaceAndOrganization(it)) }.orElse(emptyList())
    permissions.requireWorkspaces(caller, owners, setOf(AuthRoleConstants.WORKSPACE_READER), callers)
    // The legacy combined-reference check allowed different workspaces only for instance admins or no-auth.
    if (owners.distinct().size > 1) permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), callers)
  }
}

data class PublicConnectionCreateInput(
  val source: UUID,
  val destination: UUID,
) : OperationInput

@Singleton
class PublicConnectionCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.firstOrNull() as? ConnectionCreateRequest ?: throw BadRequestProblem()
    return PublicConnectionCreateInput(
      body.sourceId,
      body.destinationId,
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    permissions.requireCaller(caller, callers)
    val request = input as PublicConnectionCreateInput
    val source =
      publicConfigResource(ConfigNotFoundType.SOURCE_CONNECTION, request.source) { data.savedSourceWorkspaceAndOrganization(request.source) }
    permissions.requireWorkspaces(caller, listOf(source), publicWriteRoles, callers)
    val destination =
      publicConfigResource(ConfigNotFoundType.DESTINATION_CONNECTION, request.destination) {
        data.savedDestinationWorkspaceAndOrganization(request.destination)
      }
    if (destination != source) throw BadRequestProblem()
  }
}

data class PublicConnectionListInput(
  val workspaces: List<UUID>,
  val tags: List<UUID>,
  val includeDeleted: Boolean,
) : OperationInput

@Singleton
class PublicConnectionListRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    @Suppress("UNCHECKED_CAST")
    val workspaces = copyIds(args.firstOrNull() as? List<UUID?>)

    @Suppress("UNCHECKED_CAST")
    val tags = copyIds(args.getOrNull(1) as? List<UUID?>)
    return PublicConnectionListInput(workspaces, tags, args.getOrNull(2) as? Boolean ?: false)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    permissions.requireCaller(caller, callers)
    val request = input as PublicConnectionListInput
    val owners = request.workspaces.map { data.savedWorkspaceAndOrganization(it, request.includeDeleted) }
    if (owners.isEmpty()) {
      permissions.requireAuthenticated(
        caller,
        callers,
      )
    } else {
      permissions.requireWorkspaces(caller, owners, setOf(AuthRoleConstants.WORKSPACE_READER), callers)
    }
    // Tags are exact filters, not resources read or changed by this list. A foreign tag yields no matching rows.
  }
}

data class PublicJobInput(
  val id: Long,
) : OperationInput

abstract class PublicJobRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = PublicJobInput(args.firstOrNull() as? Long ?: throw BadRequestProblem())

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    val issuer = permissions.scopedEmbeddedIssuer(caller)
    val connection = data.replicationJobConnectionId((input as PublicJobInput).id)
    permissions.requireWorkspaces(issuer, listOf(connectionOwner(data, connection)), roles, callers)
  }
}

@Singleton
class PublicJobReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicJobRule(data, permissions, setOf(AuthRoleConstants.WORKSPACE_READER))

@Singleton
class PublicJobCancelRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicJobRule(data, permissions, setOf(AuthRoleConstants.WORKSPACE_RUNNER))

data class PublicJobCreateInput(
  val connection: UUID,
  val clear: Boolean,
) : OperationInput

@Singleton
class PublicJobCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.firstOrNull() as? JobCreateRequest ?: throw BadRequestProblem()
    return PublicJobCreateInput(publicResourceId(body.connectionId), body.jobType == JobTypeEnum.CLEAR)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    val issuer = permissions.scopedEmbeddedIssuer(caller)
    val request = input as PublicJobCreateInput
    permissions.requireWorkspaces(
      issuer,
      listOf(connectionOwner(data, request.connection)),
      if (request.clear) publicWriteRoles else setOf(AuthRoleConstants.WORKSPACE_RUNNER),
      callers,
    )
  }
}

data class PublicJobsListInput(
  val connection: Optional<UUID>,
  val workspaces: List<UUID>,
) : OperationInput

@Singleton
class PublicJobListRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    @Suppress("UNCHECKED_CAST")
    val workspaces = copyIds(args.getOrNull(4) as? List<UUID?>)
    return PublicJobsListInput(Optional.ofNullable(args.firstOrNull()).map(::publicResourceId), workspaces)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    val issuer = permissions.scopedEmbeddedIssuer(caller)
    val request = input as PublicJobsListInput
    // connectionId takes precedence over workspaceIds in the business flow and the published contract.
    val owners =
      if (request.connection.isPresent) {
        listOf(
          connectionOwner(data, request.connection.orElseThrow()),
        )
      } else {
        request.workspaces.map(data::workspaceAndOrganization)
      }
    if (owners.isEmpty()) {
      permissions.requireAuthenticated(
        issuer,
        callers,
      )
    } else {
      permissions.requireWorkspaces(issuer, owners, setOf(AuthRoleConstants.WORKSPACE_READER), callers)
    }
  }
}
