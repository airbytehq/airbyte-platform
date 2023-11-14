/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth;

/**
 * Collection of constants that defines authorization roles.
 */
public final class AuthRoleConstants {

  // TODO: replace "ADMIN" with "INSTANCE_ADMIN" and remove "EDITOR", "OWNER", etc
  // once all @Secured annotations are migrated to the new organizatin/workspace RBAC roles.
  public static final String ADMIN = "ADMIN";
  public static final String AUTHENTICATED_USER = "AUTHENTICATED_USER";
  public static final String EDITOR = "EDITOR";
  public static final String OWNER = "OWNER";
  public static final String NONE = "NONE";
  public static final String READER = "READER";

  public static final String ORGANIZATION_ADMIN = "ORGANIZATION_ADMIN";
  public static final String ORGANIZATION_EDITOR = "ORGANIZATION_EDITOR";
  public static final String ORGANIZATION_READER = "ORGANIZATION_READER";
  public static final String ORGANIZATION_MEMBER = "ORGANIZATION_MEMBER";

  public static final String WORKSPACE_ADMIN = "WORKSPACE_ADMIN";
  public static final String WORKSPACE_EDITOR = "WORKSPACE_EDITOR";
  public static final String WORKSPACE_READER = "WORKSPACE_READER";

  // the 'SELF' role is used to secure endpoints that should only be called by the user themselves.
  // For example, creating an Airbyte User record for a particular authUserId should only be done
  // by callers that have authenticated as that authUserId.
  public static final String SELF = "SELF";

  private AuthRoleConstants() {}

}
