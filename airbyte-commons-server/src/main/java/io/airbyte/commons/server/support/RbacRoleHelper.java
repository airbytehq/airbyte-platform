/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.airbyte.commons.auth.OrganizationAuthRole;
import io.airbyte.commons.auth.WorkspaceAuthRole;
import io.airbyte.commons.enums.Enums;
import io.airbyte.config.Permission;
import io.airbyte.config.persistence.PermissionPersistence;
import io.micronaut.http.HttpRequest;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Singleton
@Slf4j
public class RbacRoleHelper {

  private final AuthenticationHeaderResolver headerResolver;
  private final PermissionPersistence permissionPersistence;

  public RbacRoleHelper(final AuthenticationHeaderResolver headerResolver, final PermissionPersistence permissionPersistence) {
    this.headerResolver = headerResolver;
    this.permissionPersistence = permissionPersistence;
  }

  public Collection<String> getRbacRoles(final String userId, final HttpRequest<?> request) {
    final Map<String, String> headerMap = request.getHeaders().asMap(String.class, String.class);

    final List<UUID> workspaceIds = headerResolver.resolveWorkspace(headerMap);
    final List<UUID> organizationIds = headerResolver.resolveOrganization(headerMap);

    final Set<String> roles = new HashSet<>();

    if (workspaceIds != null && !workspaceIds.isEmpty()) {
      roles.addAll(getWorkspaceAuthRoles(userId, workspaceIds));
    }
    if (organizationIds != null && !organizationIds.isEmpty()) {
      roles.addAll(getOrganizationAuthRoles(userId, organizationIds));
    }

    return roles;
  }

  private Set<String> getWorkspaceAuthRoles(final String userId, final List<UUID> workspaceIds) {
    final Optional<WorkspaceAuthRole> minAuthRoleOptional = workspaceIds.stream()
        .map(workspaceId -> fetchWorkspacePermission(userId, workspaceId))
        .filter(Objects::nonNull)
        .map(this::convertToWorkspaceAuthRole)
        .filter(Objects::nonNull)
        .min(Comparator.comparingInt(WorkspaceAuthRole::getAuthority));

    WorkspaceAuthRole authRole = minAuthRoleOptional.orElse(WorkspaceAuthRole.NONE);
    return WorkspaceAuthRole.buildWorkspaceAuthRolesSet(authRole);
  }

  private Permission.PermissionType fetchWorkspacePermission(final String userId, final UUID workspaceId) {
    try {
      return permissionPersistence.findPermissionTypeForUserAndWorkspace(workspaceId, userId);
    } catch (IOException ex) {
      log.error("Failed to get permission for user {} and workspaces {}", userId, workspaceId, ex);
      throw new RuntimeException(ex);
    }
  }

  private WorkspaceAuthRole convertToWorkspaceAuthRole(final Permission.PermissionType permissionType) {
    try {
      return Enums.convertTo(permissionType, WorkspaceAuthRole.class);
    } catch (final Exception ex) {
      log.error("Failed to convert permissionType {} to WorkspaceAuthRole", permissionType, ex);
      return null;
    }
  }

  private Set<String> getOrganizationAuthRoles(final String userId, final List<UUID> organizationIds) {
    final Optional<OrganizationAuthRole> minAuthRoleOptional = organizationIds.stream()
        .map(orgId -> fetchOrganizationPermission(userId, orgId))
        .filter(Objects::nonNull)
        .map(this::convertToOrganizationAuthRole)
        .filter(Objects::nonNull)
        .min(Comparator.comparingInt(OrganizationAuthRole::getAuthority));

    OrganizationAuthRole authRole = minAuthRoleOptional.orElse(OrganizationAuthRole.NONE);
    return OrganizationAuthRole.buildOrganizationAuthRolesSet(authRole);
  }

  private Permission.PermissionType fetchOrganizationPermission(final String userId, final UUID orgId) {
    try {
      return permissionPersistence.findPermissionTypeForUserAndOrganization(orgId, userId);
    } catch (IOException ex) {
      log.error("Failed to get permission for user {} and organization {}", userId, orgId, ex);
      throw new RuntimeException(ex);
    }
  }

  private OrganizationAuthRole convertToOrganizationAuthRole(final Permission.PermissionType permissionType) {
    try {
      return Enums.convertTo(permissionType, OrganizationAuthRole.class);
    } catch (Exception ex) {
      log.error("Failed to convert permissionType to OrganizationAuthRole", ex);
      return null;
    }
  }

}
