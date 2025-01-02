/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.micronaut;

import io.airbyte.commons.auth.AuthRole;
import io.airbyte.commons.auth.OrganizationAuthRole;
import io.airbyte.commons.auth.RequiresAuthMode;
import io.airbyte.commons.auth.WorkspaceAuthRole;
import io.airbyte.commons.auth.config.AuthMode;
import io.airbyte.config.Application;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.data.config.InstanceAdminConfig;
import io.airbyte.data.services.ApplicationService;
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator;
import jakarta.inject.Singleton;
import jakarta.ws.rs.BadRequestException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;

@Singleton
@RequiresAuthMode(AuthMode.SIMPLE)
public class ApplicationServiceMicronautImpl implements ApplicationService {

  public static final UUID DEFAULT_AUTH_USER_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private final InstanceAdminConfig instanceAdminConfig;
  private final JwtTokenGenerator jwtTokenGenerator;

  public ApplicationServiceMicronautImpl(final InstanceAdminConfig instanceAdminConfig, final JwtTokenGenerator jwtTokenGenerator) {
    this.instanceAdminConfig = instanceAdminConfig;
    this.jwtTokenGenerator = jwtTokenGenerator;
  }

  @Override
  public List<Application> listApplicationsByUser(final AuthenticatedUser user) {
    return List.of(
        new Application()
            .withName(user.getName() + " Application")
            .withId(String.valueOf(UUID.randomUUID()))
            .withClientId(instanceAdminConfig.getClientId())
            .withClientSecret(instanceAdminConfig.getClientSecret())
            .withCreatedOn(String.valueOf(OffsetDateTime.now())));
  }

  @Override
  public String getToken(final String clientId, final String clientSecret) {
    if (clientId.equals(instanceAdminConfig.getClientId()) && clientSecret.equals(instanceAdminConfig.getClientSecret())) {
      final Set<String> roles = new HashSet<>();
      roles.addAll(AuthRole.buildAuthRolesSet(AuthRole.ADMIN));
      roles.addAll(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_ADMIN));
      roles.addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_ADMIN));
      return jwtTokenGenerator.generateToken(
          Map.of(
              "iss", "airbyte-server",
              "sub", DEFAULT_AUTH_USER_ID,
              "roles", roles,
              "exp", Instant.now().plus(24, ChronoUnit.HOURS).getEpochSecond()))
          // Necessary now that this is no longer optional, but I don't know under what conditions we could
          // end up here.
          .orElseThrow(() -> new BadRequestException("Could not generate token"));
    }
    throw new BadRequestException("Invalid client id or token");
  }

  @Override
  public Application createApplication(final AuthenticatedUser user, final String name) {
    throw new NotImplementedException();
  }

  @Override
  public Optional<Application> deleteApplication(final AuthenticatedUser user, final String applicationId) {
    throw new NotImplementedException();
  }

}
