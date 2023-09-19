/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.AuthRole;
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.server.support.AuthenticationHeaderResolver;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.User.AuthProvider;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.server.pro.KeycloakTokenValidator;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.netty.NettyHttpHeaders;
import io.micronaut.security.authentication.Authentication;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class KeycloakTokenValidatorTest {

  private static final String LOCALHOST = "http://localhost";
  private static final String URI_PATH = "/some/path";
  private static final Collection<String> DEFAULT_ROLES = AuthRole.buildAuthRolesSet(AuthRole.ADMIN);

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ORGANIZATION_ID = UUID.randomUUID();

  private KeycloakTokenValidator keycloakTokenValidator;
  private HttpClient httpClient;
  private AirbyteKeycloakConfiguration keycloakConfiguration;
  private AuthenticationHeaderResolver headerResolver;
  private PermissionPersistence permissionPersistence;

  @BeforeEach
  void setUp() {
    httpClient = mock(HttpClient.class);

    keycloakConfiguration = mock(AirbyteKeycloakConfiguration.class);
    when(keycloakConfiguration.getKeycloakUserInfoEndpoint()).thenReturn(LOCALHOST + URI_PATH);
    headerResolver = mock(AuthenticationHeaderResolver.class);
    permissionPersistence = mock(PermissionPersistence.class);

    keycloakTokenValidator = new KeycloakTokenValidator(httpClient, keycloakConfiguration, headerResolver, permissionPersistence);
  }

  @AfterEach
  void tearDown() {
    httpClient.close();
  }

  @Test
  void testValidateToken() throws Exception {
    final URI uri = new URI(LOCALHOST + URI_PATH);
    final String accessToken = """
                               eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIwM095c3pkWmNrZFd6Mk84d0ZFRkZVblJPLVJrN1lGLWZzRm1kWG1Q
                               bHdBIn0.eyJleHAiOjE2ODY4MTEwNTAsImlhdCI6MTY4NjgxMDg3MCwiYXV0aF90aW1lIjoxNjg2ODA3MTAzLCJqdGkiOiI1YzZhYTQ0
                               Yi02ZDRlLTRkMTktOWQ0NC02YmY0ZjRlMzM5OTYiLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwMDAvYXV0aC9yZWFsbXMvbWFzdGVy
                               IiwiYXVkIjpbIm1hc3Rlci1yZWFsbSIsImFjY291bnQiXSwic3ViIjoiMGYwY2JmOWEtMjRjMi00NmNjLWI1ODItZDFmZjJjMGQ1ZWY1
                               IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYWlyYnl0ZS13ZWJhcHAiLCJzZXNzaW9uX3N0YXRlIjoiN2FhOTdmYTEtYTI1Mi00NmQ0LWE0
                               NTMtOTE2Y2E3M2E4NmQ4IiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwOi8vbG9jYWxob3N0OjgwMDAiXSwicmVhbG1f
                               YWNjZXNzIjp7InJvbGVzIjpbImNyZWF0ZS1yZWFsbSIsImRlZmF1bHQtcm9sZXMtbWFzdGVyIiwib2ZmbGluZV9hY2Nlc3MiLCJhZG1p
                               biIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsibWFzdGVyLXJlYWxtIjp7InJvbGVzIjpbInZpZXctaWRl
                               bnRpdHktcHJvdmlkZXJzIiwidmlldy1yZWFsbSIsIm1hbmFnZS1pZGVudGl0eS1wcm92aWRlcnMiLCJpbXBlcnNvbmF0aW9uIiwiY3Jl
                               YXRlLWNsaWVudCIsIm1hbmFnZS11c2VycyIsInF1ZXJ5LXJlYWxtcyIsInZpZXctYXV0aG9yaXphdGlvbiIsInF1ZXJ5LWNsaWVudHMi
                               LCJxdWVyeS11c2VycyIsIm1hbmFnZS1ldmVudHMiLCJtYW5hZ2UtcmVhbG0iLCJ2aWV3LWV2ZW50cyIsInZpZXctdXNlcnMiLCJ2aWV3
                               LWNsaWVudHMiLCJtYW5hZ2UtYXV0aG9yaXphdGlvbiIsIm1hbmFnZS1jbGllbnRzIiwicXVlcnktZ3JvdXBzIl19LCJhY2NvdW50Ijp7
                               InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9w
                               ZW5pZCBwcm9maWxlIGVtYWlsIiwic2lkIjoiN2FhOTdmYTEtYTI1Mi00NmQ0LWE0NTMtOTE2Y2E3M2E4NmQ4IiwiZW1haWxfdmVyaWZp
                               ZWQiOmZhbHNlLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJhZG1pbiJ9.fTqnLrU4vtcqvqW88RGLe81EUZ48TwYt6i-EdRttPfYs6BkkR4L
                               WKbJYv0HLbJYYjalLvAuGg5ELUvyjNiZqyP4yzlCqlZvNSwtiGG8fROj5XutMyVd3jxxAsTNntHw-EX7dT9Z6_EeQlV3tVBl_yvNh-1y
                               4bujH25omDr080fmuU-4ug6PT7rxbIEjMjgQMiJQ7_B-2DXjq4bGwuB8js5kDEADJNiZjs1PLd4Cri2qC14I_CE1RcEgM4CA_oY48M13
                               DdKDaG0rH2B4zu7PD6PIMp8vgt9lq7FKh1QBfBdgDXCCbLe3RdOAua5QyeDztGyTwP7FghRLIUoK1kSbMww""";
    final String expectedUserId = "0f0cbf9a-24c2-46cc-b582-d1ff2c0d5ef5";

    final HttpRequest httpRequest = mock(HttpRequest.class);
    final NettyHttpHeaders headers = new NettyHttpHeaders();
    final String accessTokenWithoutNewline = accessToken.replace("\n", "").replace("\r", "");
    headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + accessTokenWithoutNewline);

    final String responseBody = "{\"sub\":\"0f0cbf9a-24c2-46cc-b582-d1ff2c0d5ef5\",\"preferred_username\":\"airbyte\"}";
    final HttpResponse<String> userInfoResponse = HttpResponse
        .ok(responseBody)
        .header(HttpHeaders.CONTENT_TYPE, "application/json");

    when(httpRequest.getUri()).thenReturn(uri);
    when(httpRequest.getHeaders()).thenReturn(headers);
    when(httpClient.exchange(any(HttpRequest.class), eq(String.class)))
        .thenReturn(Mono.just(userInfoResponse));

    final Publisher<Authentication> responsePublisher = keycloakTokenValidator.validateToken(accessTokenWithoutNewline, httpRequest);

    when(headerResolver.resolveWorkspace(any())).thenReturn(List.of(WORKSPACE_ID));
    when(headerResolver.resolveOrganization(any())).thenReturn(List.of(ORGANIZATION_ID));
    when(permissionPersistence.findPermissionTypeForUserAndWorkspace(WORKSPACE_ID, expectedUserId, AuthProvider.KEYCLOAK))
        .thenReturn(PermissionType.WORKSPACE_ADMIN);
    when(permissionPersistence.findPermissionTypeForUserAndOrganization(ORGANIZATION_ID, expectedUserId, AuthProvider.KEYCLOAK))
        .thenReturn(PermissionType.ORGANIZATION_READER);

    // TODO: enable this once we are ready to enable getRoles function in KeycloakTokenValidator.
    // Set<String> expectedResult = Set.of("ORGANIZATION_READER", "ADMIN", "EDITOR", "READER");
    Set<String> expectedResult = Set.of("ORGANIZATION_ADMIN", "ORGANIZATION_EDITOR", "ORGANIZATION_READER", "ADMIN", "EDITOR", "READER");

    StepVerifier.create(responsePublisher)
        .expectNextMatches(r -> matchSuccessfulResponse(r, expectedUserId, expectedResult))
        .verifyComplete();
  }

  @Test
  void testKeycloakValidationFailure() throws URISyntaxException {
    // this token is missing 'sub' claim thus the validation should fail
    final URI uri = new URI(LOCALHOST + URI_PATH);
    final String accessToken = "Bearer invalid-opHsFNA";
    final HttpRequest<?> httpRequest = mock(HttpRequest.class);
    final NettyHttpHeaders headers = new NettyHttpHeaders();
    headers.add(HttpHeaders.AUTHORIZATION, accessToken);

    final String responseBody = "{\"preferred_username\":\"airbyte\"}";
    final HttpResponse<String> userInfoResponse = HttpResponse
        .ok(responseBody)
        .header(HttpHeaders.CONTENT_TYPE, "application/json");

    when(httpRequest.getUri()).thenReturn(uri);
    when(httpRequest.getHeaders()).thenReturn(headers);
    when(httpClient.exchange(any(HttpRequest.class), eq(String.class)))
        .thenReturn(Mono.just(userInfoResponse));

    final Publisher<Authentication> responsePublisher = keycloakTokenValidator.validateToken(accessToken, httpRequest);

    // Verify the stream remains empty.
    StepVerifier.create(responsePublisher)
        .expectComplete()
        .verify();
  }

  private boolean matchSuccessfulResponse(final Authentication authentication,
                                          final String expectedUserId,
                                          final Collection<String> expectedRoles) {
    return authentication.getName().equals(expectedUserId)
        && authentication.getRoles().containsAll(expectedRoles);
  }

}
