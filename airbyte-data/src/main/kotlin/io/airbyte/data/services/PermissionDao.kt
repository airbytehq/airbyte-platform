/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Permission
import java.util.UUID

/**
 * A service that manages permissions.
 */
interface PermissionDao {
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
  @Throws(RemoveLastOrgAdminPermissionException::class)
  fun deletePermission(permissionId: UUID)

  /**
   * Delete a list of permissions by their unique ids.
   */
  @Throws(RemoveLastOrgAdminPermissionException::class)
  fun deletePermissions(permissionIds: List<UUID>)

  /**
   * Create a permission.
   */
  @Throws(PermissionRedundantException::class)
  fun createPermission(permission: Permission): Permission

  /**
   * Create a permission for a service account.
   */
  fun createServiceAccountPermission(permission: Permission): Permission

  /**
   * Update a permission
   */
  @Throws(RemoveLastOrgAdminPermissionException::class)
  fun updatePermission(permission: Permission)
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
