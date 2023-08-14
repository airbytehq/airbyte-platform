/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.BlockingHttpClient;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class KeycloakSetupTest {

  @Mock
  private HttpClient httpClient;

  @Mock
  private BlockingHttpClient blockingHttpClient;

  @Mock
  private KeycloakServer keycloakServer;

  private KeycloakSetup keycloakSetup;

  @BeforeEach
  void setup() {
    keycloakServer = mock(KeycloakServer.class);
    httpClient = mock(HttpClient.class);
    blockingHttpClient = mock(BlockingHttpClient.class);
    when(httpClient.toBlocking()).thenReturn(blockingHttpClient);
    keycloakSetup = new KeycloakSetup(httpClient, keycloakServer);
  }

  @Test
  void testRun() {
    when(keycloakServer.getKeycloakServerUrl()).thenReturn("http://localhost:8180/auth");
    when(blockingHttpClient.exchange(any(HttpRequest.class), eq(String.class)))
        .thenReturn(HttpResponse.ok());

    keycloakSetup.run();

    verify(httpClient).toBlocking();
    verify(blockingHttpClient).exchange(any(HttpRequest.class), eq(String.class));
    verify(keycloakServer).createAirbyteRealm();
    verify(keycloakServer).closeKeycloakAdminClient();
  }

  @Test
  void testRunThrowsException() {
    final String keycloakUrl = "http://localhost:8180/auth";
    when(keycloakServer.getKeycloakServerUrl()).thenReturn(keycloakUrl);
    when(httpClient.toBlocking().exchange(any(HttpRequest.class), eq(String.class)))
        .thenThrow(new HttpClientResponseException("Error", HttpResponse.serverError()));

    assertThrows(HttpClientResponseException.class, () -> keycloakSetup.run());

    verify(keycloakServer).getKeycloakServerUrl();
    verify(httpClient.toBlocking()).exchange(any(HttpRequest.class), eq(String.class));
    verify(keycloakServer, never()).createAirbyteRealm(); // Should not be called if exception is thrown
    verify(keycloakServer).closeKeycloakAdminClient();
  }

}
