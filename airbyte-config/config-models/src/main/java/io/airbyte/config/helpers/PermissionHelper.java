/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.config.Permission.PermissionType;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PermissionHelper {

  @VisibleForTesting
  protected static final Map<PermissionType, Set<PermissionType>> GRANTED_PERMISSION_TYPES_BY_DEFINED_PERMISSION_TYPE = Map.of(
      // Instance admin grants access to all permissions.
      PermissionType.INSTANCE_ADMIN, Set.of(PermissionType.values()),

      // Organization admin grants access to all organization-admin-and-lower permissions, and also all
      // workspace-admin-and-lower permissions
      // for workspaces within the organization.
      PermissionType.ORGANIZATION_ADMIN, Set.of(
          PermissionType.ORGANIZATION_ADMIN,
          PermissionType.ORGANIZATION_EDITOR,
          PermissionType.ORGANIZATION_READER,
          PermissionType.ORGANIZATION_MEMBER,
          PermissionType.WORKSPACE_OWNER,
          PermissionType.WORKSPACE_ADMIN,
          PermissionType.WORKSPACE_EDITOR,
          PermissionType.WORKSPACE_READER),

      // Organization editor grants access to all organization-editor-and-lower permissions, and also all
      // workspace-editor-and-lower permissions
      // for workspaces within the organization.
      PermissionType.ORGANIZATION_EDITOR, Set.of(
          PermissionType.ORGANIZATION_EDITOR,
          PermissionType.ORGANIZATION_READER,
          PermissionType.ORGANIZATION_MEMBER,
          PermissionType.WORKSPACE_EDITOR,
          PermissionType.WORKSPACE_READER),

      // Organization reader grants access to all organization-reader-and-lower permissions, and also
      // workspace-reader permissions
      // for workspaces within the organization.
      PermissionType.ORGANIZATION_READER, Set.of(
          PermissionType.ORGANIZATION_READER,
          PermissionType.ORGANIZATION_MEMBER,
          PermissionType.WORKSPACE_READER),

      // Organization member grants access to organization member permissions only,
      // but does not have permissions to access any workspaces and cannot grant access to workspace level
      // permission.
      PermissionType.ORGANIZATION_MEMBER, Set.of(
          PermissionType.ORGANIZATION_MEMBER),

      // Workspace owner (deprecated) is equivalent to workspace admin, and grants access to all
      // workspace-admin-and-lower permissions.
      PermissionType.WORKSPACE_OWNER, Set.of(
          PermissionType.WORKSPACE_OWNER,
          PermissionType.WORKSPACE_ADMIN,
          PermissionType.WORKSPACE_EDITOR,
          PermissionType.WORKSPACE_READER),

      // Workspace admin grants access to all workspace-admin-and-lower permissions.
      PermissionType.WORKSPACE_ADMIN, Set.of(
          PermissionType.WORKSPACE_OWNER,
          PermissionType.WORKSPACE_ADMIN,
          PermissionType.WORKSPACE_EDITOR,
          PermissionType.WORKSPACE_READER),

      // Workspace editor grants access to all workspace-editor-and-lower permissions.
      PermissionType.WORKSPACE_EDITOR, Set.of(
          PermissionType.WORKSPACE_EDITOR,
          PermissionType.WORKSPACE_READER),

      // Workspace reader grants access to just the workspace reader permission.
      PermissionType.WORKSPACE_READER, Set.of(
          PermissionType.WORKSPACE_READER));

  public static boolean definedPermissionGrantsTargetPermission(final PermissionType definedPermission, final PermissionType targetPermission) {
    return GRANTED_PERMISSION_TYPES_BY_DEFINED_PERMISSION_TYPE.get(definedPermission).contains(targetPermission);
  }

  /**
   * Returns the full set of all permission types that grant the target permission type.
   */
  public static Set<PermissionType> getPermissionTypesThatGrantTargetPermission(final PermissionType targetPermission) {
    final Set<PermissionType> grantingPermissionTypes = new HashSet<>();
    for (Map.Entry<PermissionType, Set<PermissionType>> entry : GRANTED_PERMISSION_TYPES_BY_DEFINED_PERMISSION_TYPE.entrySet()) {
      if (entry.getValue().contains(targetPermission)) {
        grantingPermissionTypes.add(entry.getKey());
      }
    }
    return grantingPermissionTypes;
  }

}
