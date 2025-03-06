/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.domain;

public enum PermissionType {
  INSTANCE_ADMIN,
  ORGANIZATION_ADMIN,
  ORGANIZATION_EDITOR,
  ORGANIZATION_READER,
  ORGANIZATION_MEMBER,
  WORKSPACE_OWNER, // TODO: remove in favor of WORKSPACE_ADMIN once Cloud Permission table is dropped
  WORKSPACE_ADMIN,
  WORKSPACE_EDITOR,
  WORKSPACE_READER
}
