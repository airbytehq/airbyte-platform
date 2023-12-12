/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum WorkspaceAuthRole {

  WORKSPACE_ADMIN(400, AuthRoleConstants.WORKSPACE_ADMIN),
  WORKSPACE_EDITOR(300, AuthRoleConstants.WORKSPACE_EDITOR),
  WORKSPACE_READER(200, AuthRoleConstants.WORKSPACE_READER),
  NONE(0, AuthRoleConstants.NONE);

  private final int authority;
  private final String label;

  WorkspaceAuthRole(final int authority, final String label) {
    this.authority = authority;
    this.label = label;
  }

  public int getAuthority() {
    return authority;
  }

  public String getLabel() {
    return label;
  }

  /**
   * Builds the set of roles based on the provided {@link WorkspaceAuthRole} value.
   * <p>
   * The generated set of auth roles contains the provided {@link WorkspaceAuthRole} (if not
   * {@code null}) and any other authentication roles with a lesser {@link #getAuthority()} value.
   * </p>
   *
   * @param authRole An {@link WorkspaceAuthRole} (may be {@code null}).
   * @return The set of {@link WorkspaceAuthRole} labels based on the provided
   *         {@link WorkspaceAuthRole}.
   */
  public static Set<String> buildWorkspaceAuthRolesSet(final WorkspaceAuthRole authRole) {
    final Set<WorkspaceAuthRole> authRoles = new HashSet<>();

    if (authRole != null) {
      authRoles.add(authRole);
      authRoles.addAll(Stream.of(values())
          .filter(role -> !NONE.equals(role))
          .filter(role -> role.getAuthority() < authRole.getAuthority())
          .collect(Collectors.toSet()));
    }

    // Sort final set by descending authority order
    return authRoles.stream()
        .sorted(Comparator.comparingInt(WorkspaceAuthRole::getAuthority))
        .map(role -> role.getLabel())
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

}
