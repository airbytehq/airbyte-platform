/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.config.Permission.PermissionType
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.workload.api.domain.ExpiredDeadlineWorkloadListRequest
import io.airbyte.workload.api.domain.LongRunningWorkloadRequest
import io.airbyte.workload.api.domain.WorkloadCancelRequest
import io.airbyte.workload.api.domain.WorkloadClaimRequest
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.airbyte.workload.api.domain.WorkloadLaunchedRequest
import io.airbyte.workload.api.domain.WorkloadListActiveRequest
import io.airbyte.workload.api.domain.WorkloadListRequest
import io.airbyte.workload.api.domain.WorkloadQueuePollRequest
import io.airbyte.workload.api.domain.WorkloadQueueQueryRequest
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import io.airbyte.workload.handler.WorkloadHandler
import io.micronaut.context.annotation.Value
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

private data class WorkloadIdInput(
  val workloadId: String,
  val claimedDataplaneId: String? = null,
) : OperationInput

private data class CreateInput(
  val workloadId: String,
  val workspaceId: UUID?,
  val organizationId: UUID?,
  val dataplaneGroup: String?,
) : OperationInput

private data class DataplaneListInput(
  val dataplaneIds: List<String>,
) : OperationInput

private data class DataplaneGroupInput(
  val dataplaneGroup: String?,
) : OperationInput

private data object GlobalInput : OperationInput

private val sharedDataplaneGroupOwners = setOf(DEFAULT_ORGANIZATION_ID, PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID)

private fun parseUuid(value: String): UUID =
  try {
    UUID.fromString(value)
  } catch (_: IllegalArgumentException) {
    throw BadRequestProblem()
  }

@Singleton
class WorkloadRoleCheck(
  private val roles: RoleResolver,
  private val permissions: PermissionHandler,
  @param:Value("\${micronaut.security.enabled:false}") private val securityEnabled: Boolean,
) {
  fun requireDataplane(
    caller: Authentication,
    organizations: Collection<UUID>,
  ) {
    if (!securityEnabled) return
    if (caller.name.isBlank() || caller.attributes.containsKey("io.airbyte.auth.workspace_scope")) throw ForbiddenProblem()

    when (TokenType.fromClaims(caller.attributes)) {
      TokenType.SERVICE_ACCOUNT, TokenType.DATAPLANE_V1 -> {
        val accountId =
          try {
            UUID.fromString(caller.name)
          } catch (_: IllegalArgumentException) {
            throw ForbiddenProblem()
          }
        val grants = permissions.getPermissionsByServiceAccountId(accountId)
        if (grants.any {
            it.permissionType == PermissionType.INSTANCE_ADMIN ||
              (it.permissionType == PermissionType.DATAPLANE && it.organizationId == null && it.workspaceId == null)
          }
        ) {
          return
        }
        if (organizations.isEmpty() ||
          organizations.any { organization ->
            grants.none { it.permissionType == PermissionType.DATAPLANE && it.organizationId == organization }
          }
        ) {
          throw ForbiddenProblem()
        }
        return
      }
      TokenType.EMBEDDED_V1 -> throw ForbiddenProblem()
      else -> Unit
    }

    val request = roles.newRequest().withClaims(caller.name, caller.attributes)
    organizations.forEach(request::withOrg)
    request.requireRole(AuthRoleConstants.DATAPLANE)
  }
}

@Singleton
class WorkloadIdRule(
  private val workloadHandler: WorkloadHandler,
  private val dataplanes: DataplaneService,
  private val groups: DataplaneGroupService,
  private val roles: WorkloadRoleCheck,
  @param:Value("\${micronaut.security.enabled:false}") private val securityEnabled: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val value = args.singleOrNull()) {
      is String -> WorkloadIdInput(value)
      is WorkloadFailureRequest -> WorkloadIdInput(value.workloadId)
      is WorkloadSuccessRequest -> WorkloadIdInput(value.workloadId)
      is WorkloadRunningRequest -> WorkloadIdInput(value.workloadId)
      is WorkloadCancelRequest -> WorkloadIdInput(value.workloadId)
      is WorkloadClaimRequest -> WorkloadIdInput(value.workloadId, value.dataplaneId)
      is WorkloadLaunchedRequest -> WorkloadIdInput(value.workloadId)
      is WorkloadHeartbeatRequest -> WorkloadIdInput(value.workloadId)
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as WorkloadIdInput
    val organizationId = workloadHandler.getWorkloadOrganizationId(request.workloadId)
    roles.requireDataplane(caller, listOfNotNull(organizationId))

    if (securityEnabled && request.claimedDataplaneId != null && organizationId != null) {
      val workload = workloadHandler.getWorkload(request.workloadId)
      if (workload.organizationId != organizationId) throw ForbiddenProblem()
      val dataplane = dataplanes.getDataplane(parseUuid(request.claimedDataplaneId))
      if (workload.dataplaneGroup != null && workload.dataplaneGroup != dataplane.dataplaneGroupId.toString()) throw ForbiddenProblem()
      val dataplaneOrganization = groups.getOrganizationIdFromDataplaneGroup(dataplane.dataplaneGroupId)
      if (dataplaneOrganization != organizationId && dataplaneOrganization !in sharedDataplaneGroupOwners) throw ForbiddenProblem()
      val tokenType = TokenType.fromClaims(caller.attributes)
      if ((tokenType == TokenType.SERVICE_ACCOUNT || tokenType == TokenType.DATAPLANE_V1) &&
        dataplane.serviceAccountId != UUID.fromString(caller.name)
      ) {
        throw ForbiddenProblem()
      }
    }
  }
}

@Singleton
class WorkloadCreateRule(
  private val workloadHandler: WorkloadHandler,
  private val workspaces: WorkspaceService,
  private val groups: DataplaneGroupService,
  private val roles: WorkloadRoleCheck,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? WorkloadCreateRequest ?: throw BadRequestProblem()
    return CreateInput(body.workloadId, body.workspaceId, body.organizationId, body.dataplaneGroup)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as CreateInput
    val savedWorkspaceOrganization = request.workspaceId?.let { workspaces.getStandardWorkspaceNoSecrets(it, false).organizationId }
    if (request.organizationId != null && request.workspaceId != null && savedWorkspaceOrganization != request.organizationId) {
      throw ForbiddenProblem()
    }
    if (request.workspaceId != null && savedWorkspaceOrganization == null) roles.requireDataplane(caller, emptyList())
    val groupOrganization = request.dataplaneGroup?.let { groups.getOrganizationIdFromDataplaneGroup(parseUuid(it)) }
    if (savedWorkspaceOrganization != null && groupOrganization != null &&
      groupOrganization != savedWorkspaceOrganization && groupOrganization !in sharedDataplaneGroupOwners
    ) {
      throw ForbiddenProblem()
    }

    val organizations = mutableListOf<UUID>()
    request.organizationId?.let(organizations::add)
    savedWorkspaceOrganization?.let(organizations::add)
    groupOrganization?.let(organizations::add)
    // Without an organization or dataplane group, create can publish to the global queue even when it names a workspace.
    val requiredOrganizations = if (request.organizationId == null && groupOrganization == null) emptyList() else organizations
    roles.requireDataplane(caller, requiredOrganizations)

    if (workloadHandler.workloadAlreadyExists(request.workloadId)) {
      val savedOrganization = workloadHandler.getWorkloadOrganizationId(request.workloadId)
      roles.requireDataplane(caller, listOfNotNull(savedOrganization))
    }
  }
}

@Singleton
class WorkloadDataplaneListRule(
  private val dataplanes: DataplaneService,
  private val groups: DataplaneGroupService,
  private val roles: WorkloadRoleCheck,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val ids =
      when (val value = args.singleOrNull()) {
        is WorkloadListRequest -> value.dataplane
        is ExpiredDeadlineWorkloadListRequest -> value.dataplane
        is WorkloadListActiveRequest -> value.dataplane
        is LongRunningWorkloadRequest -> value.dataplane
        else -> throw BadRequestProblem()
      }
    return DataplaneListInput(ids.orEmpty().toList())
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val organizations =
      (input as DataplaneListInput).dataplaneIds.map { dataplaneId ->
        val dataplane = dataplanes.getDataplane(parseUuid(dataplaneId))
        groups.getOrganizationIdFromDataplaneGroup(dataplane.dataplaneGroupId)
      }
    roles.requireDataplane(caller, organizations)
  }
}

@Singleton
class WorkloadQueueGroupRule(
  private val groups: DataplaneGroupService,
  private val roles: WorkloadRoleCheck,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val group =
      when (val value = args.singleOrNull()) {
        is WorkloadQueuePollRequest -> value.dataplaneGroup
        is WorkloadQueueQueryRequest -> value.dataplaneGroup
        else -> throw BadRequestProblem()
      }
    return DataplaneGroupInput(group)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val group = (input as DataplaneGroupInput).dataplaneGroup
    val organization = group?.let { groups.getOrganizationIdFromDataplaneGroup(parseUuid(it)) }
    roles.requireDataplane(caller, listOfNotNull(organization))
  }
}

@Singleton
class WorkloadGlobalRule(
  private val roles: WorkloadRoleCheck,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = GlobalInput

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    roles.requireDataplane(caller, emptyList())
  }
}
