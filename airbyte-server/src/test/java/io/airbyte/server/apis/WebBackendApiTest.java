/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.model.generated.ConnectionStateType;
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.WebBackendCheckUpdatesRead;
import io.airbyte.api.model.generated.WebBackendConnectionRead;
import io.airbyte.api.model.generated.WebBackendConnectionReadList;
import io.airbyte.api.model.generated.WebBackendConnectionRequestBody;
import io.airbyte.api.model.generated.WebBackendGeographiesListResult;
import io.airbyte.api.model.generated.WebBackendWorkspaceStateResult;
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper;
import io.airbyte.commons.server.errors.problems.ForbiddenProblem;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MicronautTest
@Requires(env = {Environment.TEST})
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class WebBackendApiTest extends BaseControllerTest {

  private ApiAuthorizationHelper apiAuthorizationHelper;

  // Due to some strange interaction between Micronaut 3, Java, and Kotlin, the only way to
  // mock this Kotlin dependency is to annotate it with @Bean instead of @MockBean, and to
  // declare it here instead of within the BaseControllerTest. May be able to move it
  // back to BaseControllerTest and use @MockBean after we upgrade to Micronaut 4.
  @Singleton
  @Primary
  ApiAuthorizationHelper mmAirbyteApiAuthorizationHelper() {
    return apiAuthorizationHelper;
  }

  @BeforeEach
  void setup() {
    apiAuthorizationHelper = Mockito.mock(ApiAuthorizationHelper.class);
  }

  @Test
  void testGetStateType() throws IOException {
    Mockito.when(webBackendConnectionsHandler.getStateType(Mockito.any()))
        .thenReturn(ConnectionStateType.STREAM);
    final String path = "/api/v1/web_backend/state/get_type";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
  }

  @Test
  void testWebBackendCheckUpdates() {
    Mockito.when(webBackendCheckUpdatesHandler.checkUpdates())
        .thenReturn(new WebBackendCheckUpdatesRead());
    final String path = "/api/v1/web_backend/check_updates";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
  }

  @Test
  void testWebBackendCreateConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(webBackendConnectionsHandler.webBackendCreateConnection(Mockito.any()))
        .thenReturn(new WebBackendConnectionRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/web_backend/connections/create";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new SourceDefinitionIdRequestBody()),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testWebBackendGetConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
    final String path = "/api/v1/web_backend/connections/get";

    Mockito.when(webBackendConnectionsHandler.webBackendGetConnection(Mockito.any()))
        .thenReturn(new WebBackendConnectionRead()) // first call that makes it here succeeds
        .thenReturn(new WebBackendConnectionRead()) // second call that makes it here succeeds
        .thenThrow(new ConfigNotFoundException("", "")); // third call that makes it here 404s

    // This only impacts calls where withRefreshCatalog(true) is present
    Mockito
        .doNothing() // first call that makes it here passes auth check
        .doNothing() // second call that makes it here passes auth check but 404s
        .doThrow(new ForbiddenProblem("forbidden")) // third call fails auth check and 403s
        .when(apiAuthorizationHelper).checkWorkspacePermissions(Mockito.anyString(), Mockito.any(), Mockito.any());

    // first call doesn't activate checkWorkspacePermissions because withRefreshedCatalog is false
    testEndpointStatus(
        HttpRequest.POST(path, new WebBackendConnectionRequestBody()),
        HttpStatus.OK);

    // second call activates checkWorkspacePermissions because withRefreshedCatalog is true, and passes
    // the check
    testEndpointStatus(
        HttpRequest.POST(path, new WebBackendConnectionRequestBody().connectionId(UUID.randomUUID()).withRefreshedCatalog(true)),
        HttpStatus.OK);

    // third call activates checkWorkspacePermissions because withRefreshedCatalog is true, passes it,
    // but then fails on the 404
    testErrorEndpointStatus(
        HttpRequest.POST(path, new WebBackendConnectionRequestBody().connectionId(UUID.randomUUID()).withRefreshedCatalog(true)),
        HttpStatus.NOT_FOUND);

    // fourth call activates checkWorkspacePermissions because withRefreshedCatalog is true, but fails
    // the check, so 403s
    testErrorEndpointStatus(
        HttpRequest.POST(path, new WebBackendConnectionRequestBody().connectionId(UUID.randomUUID()).withRefreshedCatalog(true)),
        HttpStatus.FORBIDDEN);
  }

  @Test
  void testWebBackendGetWorkspaceState() throws IOException {
    Mockito.when(webBackendConnectionsHandler.getWorkspaceState(Mockito.any()))
        .thenReturn(new WebBackendWorkspaceStateResult());
    final String path = "/api/v1/web_backend/workspace/state";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
  }

  @Test
  void testWebBackendListConnectionsForWorkspace() throws IOException {
    Mockito.when(webBackendConnectionsHandler.webBackendListConnectionsForWorkspace(Mockito.any()))
        .thenReturn(new WebBackendConnectionReadList());
    final String path = "/api/v1/web_backend/connections/list";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
  }

  @Test
  void testWebBackendListGeographies() {
    Mockito.when(webBackendGeographiesHandler.listGeographiesOSS())
        .thenReturn(new WebBackendGeographiesListResult());
    final String path = "/api/v1/web_backend/geographies/list";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
  }

  @Test
  void testWebBackendUpdateConnection() throws IOException, JsonValidationException, ConfigNotFoundException {
    Mockito.when(webBackendConnectionsHandler.webBackendUpdateConnection(Mockito.any()))
        .thenReturn(new WebBackendConnectionRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/web_backend/connections/update";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceIdRequestBody()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new SourceDefinitionIdRequestBody()),
        HttpStatus.NOT_FOUND);
  }

}
