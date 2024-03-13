/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.pro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.auth.AuthRole;
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled;
import io.airbyte.commons.server.support.JwtTokenParser;
import io.airbyte.commons.server.support.RbacRoleHelper;
import io.micrometer.common.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.token.validator.TokenValidator;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
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
public class KeycloakTokenValidator implements TokenValidator<HttpRequest<?>> {

  private final OkHttpClient client;
  private final AirbyteKeycloakConfiguration keycloakConfiguration;
  private final RbacRoleHelper rbacRoleHelper;

  public KeycloakTokenValidator(@Named("keycloakTokenValidatorHttpClient") final OkHttpClient okHttpClient,
                                final AirbyteKeycloakConfiguration keycloakConfiguration,
                                final RbacRoleHelper rbacRoleHelper) {
    this.client = okHttpClient;
    this.keycloakConfiguration = keycloakConfiguration;
    this.rbacRoleHelper = rbacRoleHelper;
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
    final String payload = JwtTokenParser.getJwtPayloadToken(token);
    final Collection<String> roles = new HashSet<>();

    try {
      final String jwtPayloadString = new String(Base64.getDecoder().decode(payload), StandardCharsets.UTF_8);
      final JsonNode jwtPayload = Jsons.deserialize(jwtPayloadString);
      log.debug("jwtPayload: {}", jwtPayload);

      final String authUserId = jwtPayload.get("sub").asText();
      log.debug("Performing authentication for auth user '{}'...", authUserId);

      if (StringUtils.isNotBlank(authUserId)) {
        log.debug("Successfully authenticated auth user '{}'.", authUserId);
        roles.add(AuthRole.AUTHENTICATED_USER.toString());

        log.debug("Fetching roles for auth user '{}'...", authUserId);
        roles.addAll(rbacRoleHelper.getRbacRoles(authUserId, request));

        log.debug("Authenticating user '{}' with roles {}...", authUserId, roles);
        final var userAttributeMap = JwtTokenParser.convertJwtPayloadToUserAttributes(jwtPayload);
        return Authentication.build(authUserId, roles, userAttributeMap);
      } else {
        throw new AuthenticationException("Failed to authenticate the user because the userId was blank.");
      }
    } catch (final Exception e) {
      log.error("Encountered an exception while validating the token.", e);
      throw new AuthenticationException("Failed to authenticate the user.");
    }
  }

  private Mono<Boolean> validateTokenWithKeycloak(final String token) {
    final okhttp3.Request request = new Request.Builder()
        .addHeader(org.apache.http.HttpHeaders.CONTENT_TYPE, "application/json")
        .addHeader(org.apache.http.HttpHeaders.AUTHORIZATION, "Bearer " + token)
        .url(keycloakConfiguration.getKeycloakUserInfoEndpoint())
        .get()
        .build();

    try (final Response response = client.newCall(request).execute()) {
      if (response.isSuccessful()) {
        assert response.body() != null;
        final String responseBody = response.body().string();
        return validateUserInfo(responseBody);
      } else {
        log.warn("Non-200 response from userinfo endpoint: {}", response.code());
        return Mono.just(false);
      }
    } catch (final Exception e) {
      log.error("Failed to validate access token.", e);
      return Mono.error(e);
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
