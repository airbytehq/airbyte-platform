/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.auth.roles.OrganizationAuthRole
import io.airbyte.commons.auth.roles.WorkspaceAuthRole
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.OrgMemberCount
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.services.InvalidServiceAccountPermissionRequestException
import io.airbyte.data.services.PermissionRedundantException
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.RemoveLastOrgAdminPermissionException
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class PermissionServiceDataImpl(
  private val workspaceService: WorkspaceService,
  private val permissionRepository: PermissionRepository,
) : PermissionService {
  override fun getPermission(permissionId: UUID): Permission =
    permissionRepository
      .findById(permissionId)
      .orElseThrow { ConfigNotFoundException(ConfigNotFoundType.PERMISSION, "Permission not found: $permissionId") }
      .toConfigModel()

  override fun listPermissions(): List<Permission> = permissionRepository.find().map { it.toConfigModel() }

  override fun getPermissionsForUser(userId: UUID): List<Permission> = permissionRepository.findByUserId(userId).map { it.toConfigModel() }

  override fun getPermissionsByAuthUserId(authUserId: String): List<Permission> =
    permissionRepository.queryByAuthUser(authUserId).map {
      it.toConfigModel()
    }

  override fun getPermissionsByServiceAccountId(serviceAccountId: UUID): List<Permission> =
    permissionRepository.findByServiceAccountId(serviceAccountId).map {
      it.toConfigModel()
    }

  @Transactional("config")
  override fun deletePermission(permissionId: UUID) {
    val permissionsToDelete = permissionRepository.findByIdIn(listOf(permissionId))
    throwIfDeletingLastOrgAdmin(permissionsToDelete)

    if (permissionsToDelete.isEmpty()) {
      throw ConfigNotFoundException(ConfigNotFoundType.PERMISSION, "Permission not found: $permissionId")
    }

    val user = permissionsToDelete.first().userId
    if (user == null) {
      throw ConfigNotFoundException(ConfigNotFoundType.PERMISSION, "User not found for permission: $permissionId")
    }

    val userPermissions = getPermissionsForUser(user)
    val workspacePermissionsToDelete = cascadeOrganizationPermissionDeletes(permissionsToDelete, userPermissions)
    permissionRepository.deleteByIdIn(listOf(permissionId) + workspacePermissionsToDelete)
  }

  @Transactional("config")
  override fun deletePermissions(permissionIds: List<UUID>) {
    val permissionsToDelete = permissionRepository.findByIdIn(permissionIds)
    throwIfDeletingLastOrgAdmin(permissionsToDelete)

    if (permissionsToDelete.isEmpty()) {
      throw ConfigNotFoundException(ConfigNotFoundType.PERMISSION, "Permissions not found: $permissionIds")
    }

    val user = permissionsToDelete.first().userId
    if (user == null) {
      throw ConfigNotFoundException(ConfigNotFoundType.PERMISSION, "User not found for permissions: $permissionIds")
    }

    if (permissionsToDelete.map { it.userId }.toSet().size > 1) {
      // Guard against the state where we're deleting multiple permissions for different users
      throw IllegalStateException("Permissions to delete must all belong to the same user.")
    }

    val userPermissions = getPermissionsForUser(user)
    val workspacePermissionsToDelete = cascadeOrganizationPermissionDeletes(permissionsToDelete, userPermissions)
    permissionRepository.deleteByIdIn(permissionIds + workspacePermissionsToDelete)
  }

  @Transactional("config")
  override fun createPermission(permission: Permission): Permission {
    val existingUserPermissions = getPermissionsForUser(permission.userId).toSet()

    // throw if new permission would be redundant
    if (isRedundantWorkspacePermission(permission, existingUserPermissions)) {
      throw PermissionRedundantException(
        "Permission type ${permission.permissionType} would be redundant for user ${permission.userId}. Preventing creation.",
      )
    }

    // remove any permissions that would be made redundant by adding in the new permission
    deletePermissionsMadeRedundantByPermission(permission, existingUserPermissions)

    return permissionRepository.save(permission.toEntity()).toConfigModel()
  }

  @Transactional("config")
  override fun createServiceAccountPermission(permission: Permission): Permission {
    if (permission.userId != null) {
      throw InvalidServiceAccountPermissionRequestException(
        "Service account permission can not be created when given a user id. Provide a service account id instead.",
      )
    }

    if (permission.serviceAccountId == null) {
      throw InvalidServiceAccountPermissionRequestException(
        "Missing service account id from request: $permission",
      )
    }

    return permissionRepository.save(permission.toEntity()).toConfigModel()
  }

  @Transactional("config")
  override fun updatePermission(permission: Permission) {
    // throw early if the update would remove the last org admin
    throwIfUpdateWouldRemoveLastOrgAdmin(permission)

    val otherPermissionsForUser = getPermissionsForUser(permission.userId).filter { it.permissionId != permission.permissionId }.toSet()

    // remove the permission being updated if it is now redundant.
    if (isRedundantWorkspacePermission(permission, otherPermissionsForUser)) {
      permissionRepository.deleteById(permission.permissionId)
      return
    }

    // remove any permissions that would be made redundant by adding in the newly-updated permission
    deletePermissionsMadeRedundantByPermission(permission, otherPermissionsForUser)

    permissionRepository.update(permission.toEntity()).toConfigModel()
  }

  override fun getMemberCountsForOrganizationList(orgIds: List<UUID>): List<OrgMemberCount> = permissionRepository.getMemberCountByOrgIdList(orgIds)

  private fun deletePermissionsMadeRedundantByPermission(
    permission: Permission,
    otherPermissions: Set<Permission>,
  ) {
    otherPermissions
      .filter { isRedundantWorkspacePermission(it, otherPermissions - it + permission) }
      .map { it.permissionId }
      .takeIf { it.isNotEmpty() }
      ?.let { permissionRepository.deleteByIdIn(it) }
  }

  private fun throwIfDeletingLastOrgAdmin(deletedPermissions: List<io.airbyte.data.repositories.entities.Permission>) {
    val deletedOrgAdminPermissions = deletedPermissions.filter { it.permissionType == PermissionType.organization_admin }

    // group deleted org admin permission IDs by organization ID
    val orgIdToDeletedOrgAdminPermissionIds = deletedOrgAdminPermissions.groupBy({ it.organizationId!! }, { it.id!! })

    // for each group, make sure the last org-admin isn't being deleted
    orgIdToDeletedOrgAdminPermissionIds.forEach { (orgId, deletedOrgAdminIds) ->
      throwIfDeletingLastOrgAdminForOrg(orgId, deletedOrgAdminIds.toSet())
    }
  }

  /**
   * If organization permission(s) are being deleted, get the user's permissions, gather the workspace permissions
   * for organizations where permissions are being deleted and return the workspace permission IDs to delete.
   *
   * It's assumed by this method that we've already verified that permissionsToDelete only contains permissions for a single user.
   */
  private fun cascadeOrganizationPermissionDeletes(
    permissionsToDelete: List<io.airbyte.data.repositories.entities.Permission>,
    userPermissions: List<Permission>,
  ): List<UUID> {
    val orgIds = permissionsToDelete.filter { it.organizationId != null }.map { it.organizationId }.toSet()

    // Group user permission Ids by workspaceId
    val workspaceIdToUserPermissionIds = userPermissions.filter { it.workspaceId != null }.groupBy({ it.workspaceId }, { it.permissionId })

    if (workspaceIdToUserPermissionIds.isEmpty()) {
      return emptyList()
    }

    // Get workspaceIds by their organizationId for the user's permissions
    val workspaceList = workspaceService.listStandardWorkspacesWithIds(workspaceIdToUserPermissionIds.keys.toList(), true)
    val workspaceIdsByOrgId = workspaceList.groupBy({ it.organizationId }, { it.workspaceId })

    // For workspaces in the organizations where permissions are being deleted, add that workspace permission ID to the list of permission Ids to delete.
    val workspaceIdsToDeletePermissionsFor = workspaceIdsByOrgId.filterKeys { orgIds.contains(it) }.values.flatten()
    return workspaceIdsToDeletePermissionsFor.flatMap { workspaceIdToUserPermissionIds[it] ?: emptyList() }
  }

  private fun throwIfDeletingLastOrgAdminForOrg(
    orgId: UUID,
    deletedOrgAdminPermissionIds: Set<UUID>,
  ) {
    // get all other permissions for the organization that are not being deleted
    val otherOrgPermissions = permissionRepository.findByOrganizationId(orgId).filter { it.id !in deletedOrgAdminPermissionIds }

    // if there are no other org-admin permissions remaining in the org, throw an exception
    if (otherOrgPermissions.none { it.permissionType == PermissionType.organization_admin }) {
      throw RemoveLastOrgAdminPermissionException("Cannot delete the last admin in Organization $orgId.")
    }
  }

  private fun throwIfUpdateWouldRemoveLastOrgAdmin(updatedPermission: Permission) {
    // return early if the permission is not for an organization
    val orgId = updatedPermission.organizationId ?: return

    // get the current state of the permission in the database
    val priorPermission =
      permissionRepository
        .findById(updatedPermission.permissionId)
        .orElseThrow { ConfigNotFoundException(ConfigNotFoundType.PERMISSION, "Permission not found: ${updatedPermission.permissionId}") }

    // return early if the permission was not an org admin prior to the update
    if (priorPermission.permissionType != PermissionType.organization_admin) {
      return
    }

    // get all other permissions for the organization
    val otherOrgPermissions = permissionRepository.findByOrganizationId(orgId).filter { it.id != updatedPermission.permissionId }

    // if the permission being updated is the last org admin, throw an exception
    if (otherOrgPermissions.none { it.permissionType == PermissionType.organization_admin }) {
      throw RemoveLastOrgAdminPermissionException("Cannot demote the last admin in Organization $orgId.")
    }
  }

  private fun isRedundantWorkspacePermission(
    permission: Permission,
    existingUserPermissions: Set<Permission>,
  ): Boolean {
    // only workspace permissions can be redundant
    val workspaceId = permission.workspaceId ?: return false

    // if the workspace is not in an organization, it cannot have redundant permissions
    val orgIdForWorkspace = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).orElse(null) ?: return false

    // if the user has no org-level permission, the workspace permission cannot be redundant
    val existingOrgPermission = existingUserPermissions.find { it.organizationId == orgIdForWorkspace } ?: return false

    // if the new permission is less than or equal to the existing org-level permission, it is redundant
    return getAuthority(permission.permissionType) <= getAuthority(existingOrgPermission.permissionType)
  }

  private fun getAuthority(permissionType: Permission.PermissionType): Int =
    when (permissionType) {
      Permission.PermissionType.INSTANCE_ADMIN -> throw IllegalArgumentException("INSTANCE_ADMIN permissions are not supported")
      Permission.PermissionType.DATAPLANE -> throw IllegalArgumentException("DATAPLANE permissions are not supported")
      Permission.PermissionType.ORGANIZATION_ADMIN -> OrganizationAuthRole.ORGANIZATION_ADMIN.getAuthority()
      Permission.PermissionType.ORGANIZATION_EDITOR -> OrganizationAuthRole.ORGANIZATION_EDITOR.getAuthority()
      Permission.PermissionType.ORGANIZATION_RUNNER -> OrganizationAuthRole.ORGANIZATION_RUNNER.getAuthority()
      Permission.PermissionType.ORGANIZATION_READER -> OrganizationAuthRole.ORGANIZATION_READER.getAuthority()
      Permission.PermissionType.ORGANIZATION_MEMBER -> OrganizationAuthRole.ORGANIZATION_MEMBER.getAuthority()
      Permission.PermissionType.WORKSPACE_OWNER -> WorkspaceAuthRole.WORKSPACE_ADMIN.getAuthority()
      Permission.PermissionType.WORKSPACE_ADMIN -> WorkspaceAuthRole.WORKSPACE_ADMIN.getAuthority()
      Permission.PermissionType.WORKSPACE_EDITOR -> WorkspaceAuthRole.WORKSPACE_EDITOR.getAuthority()
      Permission.PermissionType.WORKSPACE_RUNNER -> WorkspaceAuthRole.WORKSPACE_RUNNER.getAuthority()
      Permission.PermissionType.WORKSPACE_READER -> WorkspaceAuthRole.WORKSPACE_READER.getAuthority()
    }
}
