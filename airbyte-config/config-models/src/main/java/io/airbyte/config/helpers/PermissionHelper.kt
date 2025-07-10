/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.config.Permission
import java.util.EnumSet

object PermissionHelper {
  @JvmField
  @VisibleForTesting
  val GRANTED_PERMISSION_TYPES_BY_DEFINED_PERMISSION_TYPE: Map<Permission.PermissionType, Set<Permission.PermissionType>> =
    mapOf(
      // Instance admin grants access to all permissions.
      Permission.PermissionType.INSTANCE_ADMIN to Permission.PermissionType.entries.toSet(),
      // Organization admin
      Permission.PermissionType.ORGANIZATION_ADMIN to
        setOf(
          Permission.PermissionType.ORGANIZATION_ADMIN,
          Permission.PermissionType.ORGANIZATION_EDITOR,
          Permission.PermissionType.ORGANIZATION_RUNNER,
          Permission.PermissionType.ORGANIZATION_READER,
          Permission.PermissionType.ORGANIZATION_MEMBER,
          Permission.PermissionType.WORKSPACE_ADMIN,
          Permission.PermissionType.WORKSPACE_EDITOR,
          Permission.PermissionType.WORKSPACE_RUNNER,
          Permission.PermissionType.WORKSPACE_READER,
        ),
      // Organization editor
      Permission.PermissionType.ORGANIZATION_EDITOR to
        setOf(
          Permission.PermissionType.ORGANIZATION_EDITOR,
          Permission.PermissionType.ORGANIZATION_RUNNER,
          Permission.PermissionType.ORGANIZATION_READER,
          Permission.PermissionType.ORGANIZATION_MEMBER,
          Permission.PermissionType.WORKSPACE_EDITOR,
          Permission.PermissionType.WORKSPACE_RUNNER,
          Permission.PermissionType.WORKSPACE_READER,
        ),
      // Organization runner
      Permission.PermissionType.ORGANIZATION_RUNNER to
        setOf(
          Permission.PermissionType.ORGANIZATION_RUNNER,
          Permission.PermissionType.ORGANIZATION_READER,
          Permission.PermissionType.ORGANIZATION_MEMBER,
          Permission.PermissionType.WORKSPACE_RUNNER,
          Permission.PermissionType.WORKSPACE_READER,
        ),
      // Organization reader
      Permission.PermissionType.ORGANIZATION_READER to
        setOf(
          Permission.PermissionType.ORGANIZATION_READER,
          Permission.PermissionType.ORGANIZATION_MEMBER,
          Permission.PermissionType.WORKSPACE_READER,
        ),
      // Organization member
      Permission.PermissionType.ORGANIZATION_MEMBER to
        setOf(
          Permission.PermissionType.ORGANIZATION_MEMBER,
        ),
      // Workspace owner (deprecated)
      Permission.PermissionType.WORKSPACE_OWNER to
        setOf(
          Permission.PermissionType.WORKSPACE_ADMIN,
          Permission.PermissionType.WORKSPACE_EDITOR,
          Permission.PermissionType.WORKSPACE_RUNNER,
          Permission.PermissionType.WORKSPACE_READER,
        ),
      // Workspace admin
      Permission.PermissionType.WORKSPACE_ADMIN to
        setOf(
          Permission.PermissionType.WORKSPACE_ADMIN,
          Permission.PermissionType.WORKSPACE_EDITOR,
          Permission.PermissionType.WORKSPACE_RUNNER,
          Permission.PermissionType.WORKSPACE_READER,
        ),
      // Workspace editor
      Permission.PermissionType.WORKSPACE_EDITOR to
        setOf(
          Permission.PermissionType.WORKSPACE_EDITOR,
          Permission.PermissionType.WORKSPACE_RUNNER,
          Permission.PermissionType.WORKSPACE_READER,
        ),
      // Workspace runner
      Permission.PermissionType.WORKSPACE_RUNNER to
        setOf(
          Permission.PermissionType.WORKSPACE_RUNNER,
          Permission.PermissionType.WORKSPACE_READER,
        ),
      // Workspace reader
      Permission.PermissionType.WORKSPACE_READER to
        setOf(
          Permission.PermissionType.WORKSPACE_READER,
        ),
      // Dataplane
      Permission.PermissionType.DATAPLANE to
        setOf(
          Permission.PermissionType.DATAPLANE,
        ),
    )

  @JvmStatic
  fun definedPermissionGrantsTargetPermission(
    definedPermission: Permission.PermissionType,
    targetPermission: Permission.PermissionType,
  ): Boolean = GRANTED_PERMISSION_TYPES_BY_DEFINED_PERMISSION_TYPE[definedPermission]!!.contains(targetPermission)

  fun getGrantedPermissions(definedPermission: Permission.PermissionType): Set<Permission.PermissionType> =
    GRANTED_PERMISSION_TYPES_BY_DEFINED_PERMISSION_TYPE[definedPermission]!!

  /**
   * Returns the full set of all permission types that grant the target permission type.
   */
  @JvmStatic
  fun getPermissionTypesThatGrantTargetPermission(targetPermission: Permission.PermissionType): Set<Permission.PermissionType> {
    val grantingPermissionTypes: MutableSet<Permission.PermissionType> =
      EnumSet.noneOf(
        Permission.PermissionType::class.java,
      )
    for ((key, value) in GRANTED_PERMISSION_TYPES_BY_DEFINED_PERMISSION_TYPE) {
      if (value.contains(targetPermission)) {
        grantingPermissionTypes.add(key)
      }
    }
    return grantingPermissionTypes
  }
}
