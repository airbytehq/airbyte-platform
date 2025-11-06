/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Permission
import io.airbyte.data.repositories.OrgMemberCount
import java.util.UUID

/**
 * A service that manages permissions.
 */
interface PermissionService {
  /**
   * Get a permission by its unique id.
   */
  fun getPermission(permissionId: UUID): Permission

  /**
   * Get all permissions
   */
  fun listPermissions(): List<Permission>

  /**
   * Get all permissions for a given user.
   */
  fun getPermissionsForUser(userId: UUID): List<Permission>

  /**
   * Get all permissions for a given authUserId.
   */
  fun getPermissionsByAuthUserId(authUserId: String): List<Permission>

  /**
   * Get all permissions for a given service account id.
   */
  fun getPermissionsByServiceAccountId(serviceAccountId: UUID): List<Permission>

  /**
   * Delete a permission by its unique id.
   */
  fun deletePermission(permissionId: UUID)

  /**
   * Delete a list of permissions by their unique ids.
   */
  fun deletePermissions(permissionIds: List<UUID>)

  /**
   * Create a permission.
   */
  fun createPermission(permission: Permission): Permission

  /**
   * Create a permission for a service account.
   */
  fun createServiceAccountPermission(permission: Permission): Permission

  /**
   * Create a permissions for a group.
   */
  fun createGroupPermission(permission: Permission): Permission

  /**
   * Delete a group permission by its unique id.
   */
  fun deleteGroupPermission(permissionId: UUID)

  /**
   * Update a permission
   */
  fun updatePermission(permission: Permission)

  fun getMemberCountsForOrganizationList(orgIds: List<UUID>): List<OrgMemberCount>

  /**
   * Get all permissions for a given organizationId.
   */
  fun getPermissionsByOrganizationId(organizationId: UUID): List<Permission>

  /**
   * Get all permissions for a given workspace by id
   */
  fun getPermissionsByWorkspaceId(workspaceId: UUID): List<Permission>

  /**
   * Get all permissions for a given group by id
   */
  fun getPermissionsByGroupId(groupId: UUID): List<Permission>

  fun updatePermissions(permissions: List<Permission>)

  /**
   * Check if a group permission already exists for a given workspace
   *
   * @param groupId the group ID
   * @param permissionType the permission type
   * @param workspaceId the workspace ID
   * @return true if the permission exists, false otherwise
   */
  fun groupPermissionExistsForWorkspace(
    groupId: UUID,
    permissionType: Permission.PermissionType,
    workspaceId: UUID,
  ): Boolean

  /**
   * Check if a group permission already exists for a given organization
   *
   * @param groupId the group ID
   * @param permissionType the permission type
   * @param organizationId the organization ID
   * @return true if the permission exists, false otherwise
   */
  fun groupPermissionExistsForOrganization(
    groupId: UUID,
    permissionType: Permission.PermissionType,
    organizationId: UUID,
  ): Boolean
}

/**
 * Exception thrown when an operation on a permission cannot be performed because it is redundant.
 */
class PermissionRedundantException(
  message: String,
) : Exception(message)

/**
 * Exception thrown when attempting an operation on a permission that would result in an organization without any org-admin.
 */
class RemoveLastOrgAdminPermissionException(
  message: String,
) : Exception(message)

class InvalidServiceAccountPermissionRequestException(
  message: String,
) : Exception(message)

class InvalidGroupPermissionRequestException(
  message: String,
) : Exception(message)
