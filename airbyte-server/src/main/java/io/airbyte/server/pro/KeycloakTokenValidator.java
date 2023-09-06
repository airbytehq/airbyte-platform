/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.pro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.auth.AuthRole;
import io.airbyte.commons.auth.OrganizationAuthRole;
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled;
import io.airbyte.commons.server.support.AuthenticationHeaderResolver;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.User.AuthProvider;
import io.airbyte.config.persistence.PermissionPersistence;
import io.micrometer.common.util.StringUtils;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.token.validator.TokenValidator;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * Token Validator for Airbyte Pro. Performs an online validation of the token against the Keycloak
 * server.
 */
@Slf4j
@Singleton
@RequiresAirbyteProEnabled
@SuppressWarnings({"PMD.PreserveStackTrace", "PMD.UseTryWithResources", "PMD.UnusedFormalParameter", "PMD.UnusedPrivateMethod"})
public class KeycloakTokenValidator implements TokenValidator {

  private final HttpClient client;
  private final AirbyteKeycloakConfiguration keycloakConfiguration;
  private final AuthenticationHeaderResolver headerResolver;
  private final PermissionPersistence permissionPersistence;

  private static final Map<PermissionType, AuthRole> WORKSPACE_PERMISSION_TYPE_TO_AUTH_ROLE = Map.of(PermissionType.WORKSPACE_ADMIN, AuthRole.ADMIN,
      PermissionType.WORKSPACE_EDITOR, AuthRole.EDITOR, PermissionType.WORKSPACE_READER, AuthRole.READER);
  private static final Map<PermissionType, OrganizationAuthRole> ORGANIZATION_PERMISSION_TYPE_TO_AUTH_ROLE =
      Map.of(PermissionType.ORGANIZATION_ADMIN, OrganizationAuthRole.ORGANIZATION_ADMIN, PermissionType.ORGANIZATION_EDITOR,
          OrganizationAuthRole.ORGANIZATION_EDITOR, PermissionType.ORGANIZATION_READER, OrganizationAuthRole.ORGANIZATION_READER);

  public KeycloakTokenValidator(final HttpClient httpClient,
                                final AirbyteKeycloakConfiguration keycloakConfiguration,
                                final AuthenticationHeaderResolver headerResolver,
                                final PermissionPersistence permissionPersistence) {
    this.client = httpClient;
    this.keycloakConfiguration = keycloakConfiguration;
    this.headerResolver = headerResolver;
    this.permissionPersistence = permissionPersistence;
  }

  @Override
  public Publisher<Authentication> validateToken(final String token, final HttpRequest<?> request) {
    return validateTokenWithKeycloak(token)
        .flatMap(valid -> {
          if (valid) {
            return Mono.just(getAuthentication(token, request));
          } else {
            // pass to the next validator, if one exists
            log.warn("Token was not a valid Keycloak token: {}", token);
            return Mono.empty();
          }
        });
  }

  private Authentication getAuthentication(final String token, final HttpRequest<?> request) {
    final String[] tokenParts = token.split("\\.");
    final String payload = tokenParts[1];

    try {
      final String jwtPayloadString = new String(Base64.getDecoder().decode(payload), StandardCharsets.UTF_8);
      final JsonNode jwtPayload = Jsons.deserialize(jwtPayloadString);
      log.debug("jwtPayload: {}", jwtPayload);

      final String userId = jwtPayload.get("sub").asText();
      log.debug("Performing authentication for user '{}'...", userId);

      if (StringUtils.isNotBlank(userId)) {
        log.debug("Fetching roles for user '{}'...", userId);
        // For now, give all valid Keycloak users instance_admin, until we actually create an Airbyte User
        // with permissions for such logins.
        final Collection<String> roles = getInstanceAdminRoles();
        // final Collection<String> roles = getRoles(userId, request);
        log.debug("Authenticating user '{}' with roles {}...", userId, roles);
        return Authentication.build(userId, roles);
      } else {
        throw new AuthenticationException("Failed to authenticate the user because the userId was blank.");
      }
    } catch (final Exception e) {
      log.error("Encountered an exception while validating the token.", e);
      throw new AuthenticationException("Failed to authenticate the user.");
    }
  }

  private Collection<String> getInstanceAdminRoles() {
    Set<String> roles = new HashSet<>();
    roles.addAll(AuthRole.buildAuthRolesSet(AuthRole.ADMIN));
    roles.addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_ADMIN));
    return roles;
  }

  private Collection<String> getRoles(final String userId, final HttpRequest<?> request) {
    Map<String, String> headerMap = request.getHeaders().asMap(String.class, String.class);

    // We will check for permissions over organization and workspace
    final List<UUID> workspaceIds = headerResolver.resolveWorkspace(headerMap);
    final List<UUID> organizationIds = headerResolver.resolveOrganization(headerMap);

    Set<String> roles = new HashSet<>();
    roles.add(AuthRole.AUTHENTICATED_USER.toString());

    // Find the minimum permission for workspace
    if (workspaceIds != null && !workspaceIds.isEmpty()) {
      Optional<AuthRole> minAuthRoleOptional = workspaceIds.stream()
          .map(workspaceId -> {
            try {
              return permissionPersistence.findPermissionTypeForUserAndWorkspace(workspaceId, userId, AuthProvider.KEYCLOAK);
            } catch (IOException ex) {
              log.error("Failed to get permission for user {} and workspaces {}", userId, workspaceId, ex);
              throw new RuntimeException(ex);
            }
          })
          .map(permissionType -> WORKSPACE_PERMISSION_TYPE_TO_AUTH_ROLE.get(permissionType))
          .min(Comparator.comparingInt(AuthRole::getAuthority));
      AuthRole authRole = minAuthRoleOptional.orElse(AuthRole.NONE);
      roles.addAll(AuthRole.buildAuthRolesSet(authRole));
    }
    if (organizationIds != null && !organizationIds.isEmpty()) {
      Optional<OrganizationAuthRole> minAuthRoleOptional = organizationIds.stream()
          .map(organizationId -> {
            try {
              return permissionPersistence.findPermissionTypeForUserAndOrganization(organizationId, userId, AuthProvider.KEYCLOAK);
            } catch (IOException ex) {
              log.error("Failed to get permission for user {} and organization {}", userId, organizationId, ex);
              throw new RuntimeException(ex);
            }
          })
          .map(permissionType -> ORGANIZATION_PERMISSION_TYPE_TO_AUTH_ROLE.get(permissionType))
          .min(Comparator.comparingInt(OrganizationAuthRole::getAuthority));
      OrganizationAuthRole authRole = minAuthRoleOptional.orElse(OrganizationAuthRole.NONE);
      roles.addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(authRole));
    }

    return roles;
  }

  private Mono<Boolean> validateTokenWithKeycloak(final String token) {
    final HttpRequest<?> httpRequest = buildHttpRequest(token);

    return Mono.from(client.exchange(httpRequest, String.class))
        .flatMap(this::handleResponse)
        .doOnError(e -> log.error("Failed to validate access token.", e))
        .onErrorReturn(false)
        .doOnTerminate(() -> client.close());
  }

  private HttpRequest<?> buildHttpRequest(final String token) {
    return HttpRequest.GET(keycloakConfiguration.getKeycloakUserInfoEndpoint())
        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
        .contentType(MediaType.APPLICATION_JSON);
  }

  private Mono<Boolean> handleResponse(final HttpResponse<String> response) {
    if (response.getStatus().equals(HttpStatus.OK)) {
      return validateUserInfo(response.body());
    } else {
      log.warn("Non-200 response from userinfo endpoint: {}", response.getStatus());
      return Mono.just(false);
    }
  }

  private Mono<Boolean> validateUserInfo(final String responseBody) {
    final ObjectMapper objectMapper = new ObjectMapper();
    try {
      final JsonNode userInfo = objectMapper.readTree(responseBody);
      final String sub = userInfo.path("sub").asText();
      log.debug("validated Keycloak sub: {}", sub);
      return Mono.just(StringUtils.isNotBlank(sub));
    } catch (final JsonProcessingException e) {
      log.error("Failed to process JSON.", e);
      return Mono.error(e);
    }
  }

}
