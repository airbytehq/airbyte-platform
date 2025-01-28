/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.support.RbacRoleHelper;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.netty.NettyHttpHeaders;
import io.micronaut.security.authentication.Authentication;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class KeycloakTokenValidatorTest {

  private static final String LOCALHOST = "http://localhost";
  private static final String URI_PATH = "/some/path";
  private static final String INTERNAL_REALM_NAME = "_internal";

  // Note that this token was specifically constructed to include an underscore, which was a bug in
  // production
  // due to incorrect decoding.
  private static final String VALID_ACCESS_TOKEN =
      """
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
      ZWQiOmZhbHNlLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJhZW_DqyJ9
      """.replace("\n", "").replace("\r", "");

  private KeycloakTokenValidator keycloakTokenValidator;
  private OkHttpClient httpClient;
  private AirbyteKeycloakConfiguration keycloakConfiguration;
  private TokenRoleResolver tokenRoleResolver;

  @BeforeEach
  void setUp() {
    // Make sure we're covering the case where the token contains an underscore, since this used to
    // break in production.
    assert VALID_ACCESS_TOKEN.contains("_");

    httpClient = mock(OkHttpClient.class);

    keycloakConfiguration = mock(AirbyteKeycloakConfiguration.class);
    when(keycloakConfiguration.getKeycloakUserInfoEndpointForRealm(any())).thenReturn(LOCALHOST + URI_PATH);
    when(keycloakConfiguration.getInternalRealm()).thenReturn(INTERNAL_REALM_NAME);
    tokenRoleResolver = mock(TokenRoleResolver.class);

    keycloakTokenValidator = new KeycloakTokenValidator(httpClient, keycloakConfiguration, tokenRoleResolver, Optional.empty());
  }

  @Test
  void testValidateToken() throws Exception {
    final String expectedUserId = "0f0cbf9a-24c2-46cc-b582-d1ff2c0d5ef5";
    final String responseBody = "{\"sub\":\"0f0cbf9a-24c2-46cc-b582-d1ff2c0d5ef5\",\"preferred_username\":\"airbyte\"}";
    final HttpRequest<?> httpRequest = mockRequests(VALID_ACCESS_TOKEN, responseBody);

    final Publisher<Authentication> responsePublisher = keycloakTokenValidator.validateToken(VALID_ACCESS_TOKEN, httpRequest);

    final Set<String> mockedRoles =
        Set.of("ORGANIZATION_ADMIN", "ORGANIZATION_EDITOR", "ORGANIZATION_READER", "ORGANIZATION_MEMBER", "ADMIN", "EDITOR", "READER");

    when(tokenRoleResolver.resolveRoles(eq(expectedUserId), any(HttpRequest.class)))
        .thenReturn(mockedRoles);

    StepVerifier.create(responsePublisher)
        .expectNextMatches(r -> matchSuccessfulResponse(r, expectedUserId, mockedRoles))
        .verifyComplete();
  }

  @Test
  void testInternalServiceAccountIsInstanceAdmin() throws Exception {
    final String sub = UUID.randomUUID().toString();
    final String issuer = "/auth/realms/" + INTERNAL_REALM_NAME;
    final String clientName = "airbyte-workload-client";
    final String accessToken = JWT.create().withSubject(sub).withIssuer(issuer).withClaim("azp", clientName).sign(Algorithm.none());

    final String responseBody = Jsons.serialize(Map.of("sub", sub, "iss", issuer, "azp", clientName));
    final HttpRequest<?> httpRequest = mockRequests(accessToken, responseBody);

    final Publisher<Authentication> responsePublisher = keycloakTokenValidator.validateToken(accessToken, httpRequest);

    verifyNoInteractions(tokenRoleResolver);

    StepVerifier.create(responsePublisher)
        .expectNextMatches(r -> matchSuccessfulResponse(r, clientName, RbacRoleHelper.getInstanceAdminRoles()))
        .verifyComplete();
  }

  @Test
  void testKeycloakValidationFailureNoSubClaim() throws Exception {
    final HttpRequest<?> httpRequest = mockRequests(VALID_ACCESS_TOKEN, "{\"preferred_username\":\"airbyte\"}");

    final Publisher<Authentication> responsePublisher = keycloakTokenValidator.validateToken(VALID_ACCESS_TOKEN, httpRequest);

    // Verify the stream remains empty.
    StepVerifier.create(responsePublisher)
        .expectComplete()
        .verify();
  }

  @Test
  void testTokenWithNoRealmIsPassedToNextValidator() throws URISyntaxException {
    final URI uri = new URI(LOCALHOST + URI_PATH);

    final String blankJWT = JWT.create().sign(Algorithm.none());

    // set up mocked incoming request
    final HttpRequest<?> httpRequest = mock(HttpRequest.class);
    final NettyHttpHeaders headers = new NettyHttpHeaders();
    headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + blankJWT);
    when(httpRequest.getUri()).thenReturn(uri);
    when(httpRequest.getHeaders()).thenReturn(headers);

    final Publisher<Authentication> responsePublisher = keycloakTokenValidator.validateToken(blankJWT, httpRequest);
    StepVerifier.create(responsePublisher)
        .verifyComplete();
  }

  private HttpRequest<?> mockRequests(final String jwtToken, final String userInfoPayload) throws IOException {
    final URI uri = URI.create(LOCALHOST + URI_PATH);

    // set up mocked incoming request
    final HttpRequest<?> httpRequest = mock(HttpRequest.class);
    final NettyHttpHeaders headers = new NettyHttpHeaders();
    headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + jwtToken);
    when(httpRequest.getUri()).thenReturn(uri);
    when(httpRequest.getHeaders()).thenReturn(headers);

    // set up mock http response from Keycloak
    final Response userInfoResponse = mock(Response.class);
    final ResponseBody userInfoResponseBody = mock(ResponseBody.class);
    when(userInfoResponseBody.string()).thenReturn(userInfoPayload);
    when(userInfoResponse.body()).thenReturn(userInfoResponseBody);
    when(userInfoResponse.code()).thenReturn(200);
    when(userInfoResponse.isSuccessful()).thenReturn(true);

    final Call call = mock(Call.class);
    when(call.execute()).thenReturn(userInfoResponse);
    when(httpClient.newCall(any(Request.class))).thenReturn(call);

    return httpRequest;
  }

  private boolean matchSuccessfulResponse(final Authentication authentication,
                                          final String expectedUserId,
                                          final Collection<String> expectedRoles) {
    return authentication.getName().equals(expectedUserId)
        && authentication.getRoles().containsAll(expectedRoles);
  }

}
