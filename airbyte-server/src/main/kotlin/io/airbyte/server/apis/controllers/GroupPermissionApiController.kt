/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.GroupPermissionAlreadyExistsProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.server.generated.apis.GroupPermissionApi
import io.airbyte.api.server.generated.models.GroupIdRequestBody
import io.airbyte.api.server.generated.models.GroupPermissionCreate
import io.airbyte.api.server.generated.models.GroupPermissionIdRequestBody
import io.airbyte.api.server.generated.models.GroupPermissionRead
import io.airbyte.api.server.generated.models.GroupPermissionReadList
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.config.Group
import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.GroupId
import io.airbyte.server.apis.mappers.toGroupPermissionRead
import io.airbyte.server.helpers.GroupPermissionValidator
import io.airbyte.server.helpers.GroupsEntitlementHelper
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.UUID

private const val GROUP_RESOURCE_TYPE = "group"
private const val PERMISSION_RESOURCE_TYPE = "permission"

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class GroupPermissionApiController(
  private val groupService: GroupService,
  private val permissionService: PermissionService,
  private val roleResolver: RoleResolver,
  private val groupsEntitlementHelper: GroupsEntitlementHelper,
  private val groupPermissionValidator: GroupPermissionValidator,
) : GroupPermissionApi {
  override fun listGroupPermissions(groupIdRequestBody: GroupIdRequestBody): GroupPermissionReadList {
    getAuthorizedGroup(groupIdRequestBody.groupId)

    return GroupPermissionReadList(
      permissions =
        permissionService
          .getPermissionsByGroupId(groupIdRequestBody.groupId)
          .map { it.toGroupPermissionRead() },
    )
  }

  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun createGroupPermission(groupPermissionCreate: GroupPermissionCreate): GroupPermissionRead {
    val group = getAuthorizedGroup(groupPermissionCreate.groupId)
    val workspaceId = groupPermissionCreate.workspaceId
    val organizationId = groupPermissionCreate.organizationId

    val permissionType = Permission.PermissionType.valueOf(groupPermissionCreate.permissionType.name)

    // Validate type/scope coherence, workspace/organization existence, cross-organization scope,
    // and the target-scope role.
    groupPermissionValidator.validateScope(group, permissionType, workspaceId, organizationId)

    if (groupPermissionValidator.isDuplicate(groupPermissionCreate.groupId, permissionType, workspaceId, organizationId)) {
      val resourceType = if (workspaceId != null) "workspace" else "organization"
      val resourceId = workspaceId ?: organizationId
      throw GroupPermissionAlreadyExistsProblem(
        ProblemMessageData().message(
          "Group already has ${groupPermissionCreate.permissionType} permission for $resourceType $resourceId",
        ),
      )
    }

    val permission =
      Permission()
        .withGroupId(groupPermissionCreate.groupId)
        .withPermissionType(permissionType)
        .withWorkspaceId(workspaceId)
        .withOrganizationId(organizationId)

    return permissionService.createGroupPermission(permission).toGroupPermissionRead()
  }

  @Status(HttpStatus.NO_CONTENT)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun deleteGroupPermission(groupPermissionIdRequestBody: GroupPermissionIdRequestBody) {
    getAuthorizedGroup(groupPermissionIdRequestBody.groupId)

    val permission =
      try {
        permissionService.getPermission(groupPermissionIdRequestBody.permissionId)
      } catch (_: ConfigNotFoundException) {
        throw ResourceNotFoundProblem(
          ProblemResourceData()
            .resourceType(PERMISSION_RESOURCE_TYPE)
            .resourceId(groupPermissionIdRequestBody.permissionId.toString()),
        )
      }

    if (permission.groupId == null || permission.groupId != groupPermissionIdRequestBody.groupId) {
      throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType(PERMISSION_RESOURCE_TYPE)
          .resourceId(groupPermissionIdRequestBody.permissionId.toString()),
      )
    }

    permissionService.deleteGroupPermission(groupPermissionIdRequestBody.permissionId)
  }

  private fun getAuthorizedGroup(groupId: UUID): Group {
    val group =
      groupService.getGroup(GroupId(groupId))
        ?: throw ResourceNotFoundProblem(
          ProblemResourceData()
            .resourceType(GROUP_RESOURCE_TYPE)
            .resourceId(groupId.toString()),
        )

    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, group.organizationId.value.toString())
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)
    groupsEntitlementHelper.ensureEntitled(group.organizationId)

    return group
  }
}
