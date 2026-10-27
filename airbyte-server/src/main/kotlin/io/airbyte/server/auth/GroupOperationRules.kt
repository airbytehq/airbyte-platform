/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.server.generated.models.GroupIdRequestBody
import io.airbyte.api.server.generated.models.GroupMemberRequestBody
import io.airbyte.api.server.generated.models.GroupPermissionCreate
import io.airbyte.api.server.generated.models.GroupPermissionIdRequestBody
import io.airbyte.api.server.generated.models.GroupUpdate
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.config.Permission
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.MembershipAuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

sealed interface GroupOperationInput : OperationInput {
  val groupId: UUID

  data class Saved(
    override val groupId: UUID,
  ) : GroupOperationInput

  data class Member(
    override val groupId: UUID,
    val userId: UUID,
  ) : GroupOperationInput

  data class Grant(
    override val groupId: UUID,
    val workspace: Optional<UUID>,
    val organization: Optional<UUID>,
    val type: Permission.PermissionType,
  ) : GroupOperationInput

  data class PermissionDelete(
    override val groupId: UUID,
    val permissionId: UUID,
  ) : GroupOperationInput
}

private fun groupInput(args: Array<out Any?>): GroupOperationInput =
  when (val body = args.singleOrNull()) {
    is GroupIdRequestBody -> GroupOperationInput.Saved(body.groupId)
    is GroupUpdate -> GroupOperationInput.Saved(body.groupId)
    is GroupMemberRequestBody -> GroupOperationInput.Member(body.groupId, body.userId)
    is GroupPermissionCreate ->
      GroupOperationInput.Grant(
        body.groupId,
        Optional.ofNullable(body.workspaceId),
        Optional.ofNullable(body.organizationId),
        Permission.PermissionType.valueOf(body.permissionType.name),
      )
    is GroupPermissionIdRequestBody -> GroupOperationInput.PermissionDelete(body.groupId, body.permissionId)
    else -> throw BadRequestProblem()
  }

abstract class GroupAdminOperationRule(
  protected val data: AuthorizationDataService,
  protected val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): GroupOperationInput = groupInput(args)

  protected fun requireGroupAdmin(
    caller: Authentication,
    groupId: UUID,
  ): UUID {
    // Existing saved-group checks use withCurrentUser(), not worker-token instance-admin labels.
    permissions.requireAuthenticated(caller, setOf(OperationCaller.USER))
    val organization =
      try {
        data.groupOrganization(groupId)
      } catch (
        _: AuthorizationResourceNotFoundException,
      ) {
        throw missingGroupResource("group", groupId)
      }
    permissions.requireOrganizations(caller, listOf(organization), setOf(AuthRoleConstants.ORGANIZATION_ADMIN), setOf(OperationCaller.USER))
    return organization
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireGroupAdmin(caller, (input as GroupOperationInput).groupId)
  }
}

@Singleton
class GroupSavedAdminRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : GroupAdminOperationRule(data, permissions)

@Singleton
class GroupMemberAddRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
  private val membership: MembershipAuthorizationDataService,
) : GroupAdminOperationRule(data, permissions) {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as GroupOperationInput.Member
    val organization = requireGroupAdmin(caller, request.groupId)
    val member =
      try {
        membership.isDirectOrganizationMember(request.userId, organization)
      } catch (
        _: AuthorizationResourceNotFoundException,
      ) {
        false
      }
    if (!member) throw missingGroupResource("user", request.userId)
  }
}

/** The user ID is an exact deletion filter; absent membership remains an idempotent no-op. */
@Singleton
class GroupMemberRemoveRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : GroupAdminOperationRule(data, permissions)

@Singleton
class GroupPermissionCreateRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : GroupAdminOperationRule(data, permissions) {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as GroupOperationInput.Grant
    val organization = requireGroupAdmin(caller, request.groupId)
    if (request.workspace.isPresent == request.organization.isPresent) throw BadRequestProblem()
    if (request.workspace.isPresent) {
      if (!request.type.name.startsWith("WORKSPACE_")) throw BadRequestProblem()
      val workspace = request.workspace.orElseThrow()
      val owner =
        try {
          data.workspaceAndOrganization(workspace)
        } catch (
          _: AuthorizationResourceNotFoundException,
        ) {
          throw missingGroupResource("workspace", workspace)
        }
      if (owner.organizationId != organization) throw missingGroupResource("workspace", workspace)
      permissions.requireWorkspaces(caller, listOf(owner), setOf(AuthRoleConstants.WORKSPACE_ADMIN), setOf(OperationCaller.USER))
    } else {
      if (!request.type.name.startsWith("ORGANIZATION_")) throw BadRequestProblem()
      val target = request.organization.orElseThrow()
      if (target != organization) throw missingGroupResource("organization", target)
      try {
        data.organization(target)
      } catch (_: AuthorizationResourceNotFoundException) {
        throw missingGroupResource("organization", target)
      }
      permissions.requireOrganizations(caller, listOf(target), setOf(AuthRoleConstants.ORGANIZATION_ADMIN), setOf(OperationCaller.USER))
    }
  }
}

@Singleton
class GroupPermissionDeleteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
  private val membership: MembershipAuthorizationDataService,
) : GroupAdminOperationRule(data, permissions) {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as GroupOperationInput.PermissionDelete
    requireGroupAdmin(caller, request.groupId)
    val group =
      try {
        membership.permissionGroup(request.permissionId)
      } catch (
        _: AuthorizationResourceNotFoundException,
      ) {
        throw missingGroupResource("permission", request.permissionId)
      }
    if (group.isEmpty || group.orElseThrow() != request.groupId) throw missingGroupResource("permission", request.permissionId)
  }
}

private fun missingGroupResource(
  type: String,
  id: UUID,
): ResourceNotFoundProblem = ResourceNotFoundProblem(ProblemResourceData().resourceType(type).resourceId(id.toString()))
