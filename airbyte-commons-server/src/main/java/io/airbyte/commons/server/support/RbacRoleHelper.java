/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.airbyte.commons.auth.AuthRole;
import io.airbyte.commons.auth.AuthRoleConstants;
import io.airbyte.commons.auth.OrganizationAuthRole;
import io.airbyte.commons.auth.WorkspaceAuthRole;
import io.airbyte.commons.enums.Enums;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.helpers.PermissionHelper;
import io.airbyte.config.persistence.PermissionPersistence;
import io.micronaut.http.HttpRequest;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class RbacRoleHelper {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AuthenticationHeaderResolver headerResolver;
  private final PermissionPersistence permissionPersistence;

  public RbacRoleHelper(final AuthenticationHeaderResolver headerResolver, final PermissionPersistence permissionPersistence) {
    this.headerResolver = headerResolver;
    this.permissionPersistence = permissionPersistence;
  }

  public Collection<String> getRbacRoles(final String authUserId, final Map<String, String> headerMap) {
    final List<UUID> workspaceIds = headerResolver.resolveWorkspace(headerMap);
    final List<UUID> organizationIds = headerResolver.resolveOrganization(headerMap);
    final Set<String> targetAuthUserIds = headerResolver.resolveAuthUserIds(headerMap);

    final Set<String> roles = new HashSet<>();

    if (workspaceIds != null && !workspaceIds.isEmpty()) {
      roles.addAll(getWorkspaceAuthRoles(authUserId, workspaceIds));
    }
    if (organizationIds != null && !organizationIds.isEmpty()) {
      roles.addAll(getOrganizationAuthRoles(authUserId, organizationIds));
    }
    // We need to add all underlying granted roles based on the actual user roles. For example,
    // org_admin role will add all org AND workspace level roles.
    final Set<String> allRoles = new HashSet<>(roles);
    roles.stream()
        .filter(userRole -> !AuthRoleConstants.NONE.equals(userRole))
        .map(userRole -> PermissionHelper.getGrantedPermissions(Permission.PermissionType.valueOf(userRole)))
        .flatMap(Set::stream)
        .map(PermissionType::name)
        .forEach(allRoles::add);
    if (targetAuthUserIds != null && targetAuthUserIds.contains(authUserId)) {
      allRoles.add(AuthRoleConstants.SELF);
    }
    try {
      if (permissionPersistence.isAuthUserInstanceAdmin(authUserId)) {
        allRoles.addAll(getInstanceAdminRoles());
      }
    } catch (final IOException ex) {
      log.error("Failed to get instance admin roles for user {}", authUserId, ex);
      throw new RuntimeException(ex);
    }
    return allRoles;
  }

  public Collection<String> getRbacRoles(final String authUserId, final HttpRequest<?> request) {
    final Map<String, String> headerMap = request.getHeaders().asMap(String.class, String.class);
    return getRbacRoles(authUserId, headerMap);
  }

  public static Set<String> getInstanceAdminRoles() {
    final Set<String> roles = new HashSet<>();
    roles.addAll(AuthRole.buildAuthRolesSet(AuthRole.ADMIN));
    roles.addAll(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_ADMIN));
    roles.addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_ADMIN));
    // For now, SELF is intentionally excluded from instance admin roles. If a user-centric endpoint
    // should be
    // callable by an instance admin, then the endpoint should be annotated with ADMIN in addition to
    // SELF.
    return roles;
  }

  private Set<String> getWorkspaceAuthRoles(final String authUserId, final List<UUID> workspaceIds) {
    final List<PermissionType> workspacePermissionTypes = workspaceIds.stream()
        .map(workspaceId -> fetchWorkspacePermission(authUserId, workspaceId))
        .toList();

    // if any workspace permission type is null, the user should not have any workspace roles for this
    // request at all.
    if (workspacePermissionTypes.stream().anyMatch(Objects::isNull)) {
      return WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.NONE);
    }

    final Optional<WorkspaceAuthRole> minAuthRoleOptional = workspacePermissionTypes.stream()
        .map(this::convertToWorkspaceAuthRole)
        .min(Comparator.comparingInt(WorkspaceAuthRole::getAuthority));

    final WorkspaceAuthRole authRole = minAuthRoleOptional.orElse(WorkspaceAuthRole.NONE);
    return WorkspaceAuthRole.buildWorkspaceAuthRolesSet(authRole);
  }

  private Permission.PermissionType fetchWorkspacePermission(final String authUserId, final UUID workspaceId) {
    try {
      return permissionPersistence.findPermissionTypeForUserAndWorkspace(workspaceId, authUserId);
    } catch (final IOException ex) {
      log.error("Failed to get permission for user {} and workspaces {}", authUserId, workspaceId, ex);
      throw new RuntimeException(ex);
    }
  }

  private WorkspaceAuthRole convertToWorkspaceAuthRole(final Permission.PermissionType permissionType) {
    return Enums.convertTo(permissionType, WorkspaceAuthRole.class);
  }

  private Set<String> getOrganizationAuthRoles(final String authUserId, final List<UUID> organizationIds) {
    final List<PermissionType> orgPermissionTypes = organizationIds.stream()
        .map(orgId -> fetchOrganizationPermission(authUserId, orgId))
        .toList();

    // if any org permission type is null, the user should not have any org roles for this request at
    // all.
    if (orgPermissionTypes.stream().anyMatch(Objects::isNull)) {
      return OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.NONE);
    }

    final Optional<OrganizationAuthRole> minAuthRoleOptional = orgPermissionTypes.stream()
        .map(this::convertToOrganizationAuthRole)
        .min(Comparator.comparingInt(OrganizationAuthRole::getAuthority));

    final OrganizationAuthRole authRole = minAuthRoleOptional.orElse(OrganizationAuthRole.NONE);
    return OrganizationAuthRole.buildOrganizationAuthRolesSet(authRole);
  }

  private Permission.PermissionType fetchOrganizationPermission(final String authUserId, final UUID orgId) {
    try {
      return permissionPersistence.findPermissionTypeForUserAndOrganization(orgId, authUserId);
    } catch (final IOException ex) {
      log.error("Failed to get permission for user {} and organization {}", authUserId, orgId, ex);
      throw new RuntimeException(ex);
    }
  }

  private OrganizationAuthRole convertToOrganizationAuthRole(final Permission.PermissionType permissionType) {
    return Enums.convertTo(permissionType, OrganizationAuthRole.class);
  }

}
