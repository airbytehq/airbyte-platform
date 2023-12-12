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

/**
 * This enum describes the Organization auth levels for a given resource. A user will have
 * organization leveled auth role and workspace leveled auth roles. See AuthRole.java for more
 * information.
 */
public enum OrganizationAuthRole {

  ORGANIZATION_ADMIN(400, AuthRoleConstants.ORGANIZATION_ADMIN),
  ORGANIZATION_EDITOR(300, AuthRoleConstants.ORGANIZATION_EDITOR),
  ORGANIZATION_READER(200, AuthRoleConstants.ORGANIZATION_READER),
  ORGANIZATION_MEMBER(100, AuthRoleConstants.ORGANIZATION_MEMBER),
  NONE(0, AuthRoleConstants.NONE);

  private final int authority;
  private final String label;

  OrganizationAuthRole(final int authority, final String label) {
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
   * Builds the set of roles based on the provided {@link OrganizationAuthRole} value.
   * <p>
   * The generated set of auth roles contains the provided {@link OrganizationAuthRole} (if not
   * {@code null}) and any other authentication roles with a lesser {@link #getAuthority()} value.
   * </p>
   *
   * @param authRole An {@link OrganizationAuthRole} (may be {@code null}).
   * @return The set of {@link OrganizationAuthRole} labels based on the provided
   *         {@link OrganizationAuthRole}.
   */
  public static Set<String> buildOrganizationAuthRolesSet(final OrganizationAuthRole authRole) {
    final Set<OrganizationAuthRole> authRoles = new HashSet<>();

    if (authRole != null) {
      authRoles.add(authRole);
      authRoles.addAll(Stream.of(values())
          .filter(role -> !NONE.equals(role))
          .filter(role -> role.getAuthority() < authRole.getAuthority())
          .collect(Collectors.toSet()));
    }

    // Sort final set by descending authority order
    return authRoles.stream()
        .sorted(Comparator.comparingInt(OrganizationAuthRole::getAuthority))
        .map(role -> role.getLabel())
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

}
