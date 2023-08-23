/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import java.util.Map;

/**
 * Helper class to merge permissions.
 */
public class PermissionMerger {

  /**
   * We now have the following permissions: INSTANCE ADMIN WORKSPACE LEVEL: ADMIN, EDITOR, READER
   * ORGANIZATION LEVEL: ADMIN, EDITOR, READER.
   *
   * We define that INSTANCE ADMIN has the highest permission. ADMIN > EDITOR > READER, and we use
   * WORKSPACE/ORGANIZATION as tiebreaker: ORGANIZATION has higher permission.
   */
  private static final Map<PermissionType, Integer> PERMISSION_RANKING_REFERENCE = Map.of(PermissionType.INSTANCE_ADMIN, 1000,
      PermissionType.ORGANIZATION_ADMIN, 501, PermissionType.ORGANIZATION_EDITOR, 301, PermissionType.ORGANIZATION_READER, 101,
      PermissionType.WORKSPACE_ADMIN, 500, PermissionType.WORKSPACE_EDITOR, 300, PermissionType.WORKSPACE_READER, 100);

  /**
   * Merge two permissions. Always pick the higher permission.
   */
  public static Permission pickHigherPermission(Permission permission1, Permission permission2) {
    if (PERMISSION_RANKING_REFERENCE.get(permission1.getPermissionType()) > PERMISSION_RANKING_REFERENCE.get(permission2.getPermissionType())) {
      return permission1;
    } else {
      return permission2;
    }
  }

}
