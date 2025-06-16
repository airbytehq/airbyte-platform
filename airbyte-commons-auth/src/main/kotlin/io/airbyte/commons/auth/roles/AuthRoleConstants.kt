/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.roles

object AuthRoleConstants {
  // TODO: replace "ADMIN" with "INSTANCE_ADMIN" and remove "EDITOR", "OWNER", etc
  // once all @Secured annotations are migrated to the new organization/workspace RBAC roles.
  const val ADMIN = "ADMIN"
  const val INSTANCE_ADMIN = "INSTANCE_ADMIN"
  const val AUTHENTICATED_USER = "AUTHENTICATED_USER"
  const val EDITOR = "EDITOR"
  const val OWNER = "OWNER"
  const val NONE = "NONE"
  const val READER = "READER"

  const val ORGANIZATION_ADMIN = "ORGANIZATION_ADMIN"
  const val ORGANIZATION_EDITOR = "ORGANIZATION_EDITOR"
  const val ORGANIZATION_RUNNER = "ORGANIZATION_RUNNER"
  const val ORGANIZATION_READER = "ORGANIZATION_READER"
  const val ORGANIZATION_MEMBER = "ORGANIZATION_MEMBER"

  const val WORKSPACE_ADMIN = "WORKSPACE_ADMIN"
  const val WORKSPACE_EDITOR = "WORKSPACE_EDITOR"
  const val WORKSPACE_RUNNER = "WORKSPACE_RUNNER"
  const val WORKSPACE_READER = "WORKSPACE_READER"

  const val EMBEDDED_END_USER = "EMBEDDED_END_USER"

  const val DATAPLANE = "DATAPLANE"

  // the 'SELF' role is used to secure endpoints that should only be called by the user themselves.
  // For example, creating an Airbyte User record for a particular authUserId should only be done
  // by callers that have authenticated as that authUserId.
  const val SELF = "SELF"
}
