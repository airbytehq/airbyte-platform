/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.pro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.auth.AuthRole;
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled;
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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
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
@SuppressWarnings({"PMD.PreserveStackTrace", "PMD.UseTryWithResources"})
public class KeycloakTokenValidator implements TokenValidator {

  private final HttpClient client;
  private final AirbyteKeycloakConfiguration keycloakConfiguration;

  public KeycloakTokenValidator(final HttpClient httpClient,
                                final AirbyteKeycloakConfiguration keycloakConfiguration) {
    this.client = httpClient;
    this.keycloakConfiguration = keycloakConfiguration;
  }

  @Override
  public Publisher<Authentication> validateToken(final String token, final HttpRequest<?> request) {
    return validateTokenWithKeycloak(token)
        .flatMap(valid -> {
          if (valid) {
            return Mono.just(getAuthentication(token));
          } else {
            // pass to the next validator, if one exists
            log.warn("Token was not a valid Keycloak token: {}", token);
            return Mono.empty();
          }
        });
  }

  private Authentication getAuthentication(final String token) {
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
        final Collection<String> roles = getRoles(userId);
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

  /**
   * For now, we are granting ADMIN to all authenticated users. This will change with the introduction
   * of RBAC.
   */
  private Collection<String> getRoles(final String userId) {
    log.debug("Granting ADMIN role to user {}", userId);
    return AuthRole.buildAuthRolesSet(AuthRole.ADMIN);
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
