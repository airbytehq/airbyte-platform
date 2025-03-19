/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth;

/**
 * This interface allows us to approximate a discriminated union of the implementers (AuthRole,
 * WorkspaceAuthRole, and OrganizationAuthRole). This allows our the
 * ApiAuthorizationHelper.kt#ensureUserHasAnyRequiredRoleOrThrow to accept a list containing any of
 * the types without forcing us to use strings for everything.
 */
public interface AuthRoleInterface {

  int getAuthority();

  String getLabel();

}
