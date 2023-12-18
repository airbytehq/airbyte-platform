/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.failsafe.RetryPolicy;
import io.airbyte.api.client2.AirbyteApiClient2;
import io.airbyte.api.client2.model.generated.CustomDestinationDefinitionCreate;
import io.airbyte.api.client2.model.generated.CustomSourceDefinitionCreate;
import io.airbyte.api.client2.model.generated.DestinationDefinitionCreate;
import io.airbyte.api.client2.model.generated.DestinationDefinitionIdRequestBody;
import io.airbyte.api.client2.model.generated.DestinationDefinitionRead;
import io.airbyte.api.client2.model.generated.SourceDefinitionCreate;
import io.airbyte.api.client2.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.client2.model.generated.SourceDefinitionRead;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@TestInstance(Lifecycle.PER_CLASS)
class VersioningAcceptanceTests {

  private static AirbyteApiClient2 apiClient2;
  private static UUID workspaceId;
  private static final String AIRBYTE_SERVER_HOST = Optional.ofNullable(System.getenv("AIRBYTE_SERVER_HOST")).orElse("http://localhost:8001");

  @BeforeAll
  static void init() throws IOException, URISyntaxException {
    final RetryPolicy<okhttp3.Response> policy = RetryPolicy.<okhttp3.Response>builder()
        .handle(Throwable.class)
        .withMaxAttempts(5)
        .withBackoff(Duration.ofSeconds(1), Duration.ofSeconds(10)).build();

    final OkHttpClient client = new OkHttpClient.Builder().readTimeout(Duration.ofSeconds(20)).build();
    apiClient2 = new AirbyteApiClient2(String.format("%s/api", AIRBYTE_SERVER_HOST), policy, client);

    workspaceId = apiClient2.getWorkspaceApi().listWorkspaces().getWorkspaces().get(0).getWorkspaceId();
  }

  @ParameterizedTest
  @CsvSource({
    "2.1.1, 0.2.0",
    "2.1.2, 0.2.1",
  })
  void testCreateSourceSpec(final String dockerImageTag, final String expectedProtocolVersion)
      throws URISyntaxException, IOException {
    final CustomSourceDefinitionCreate srcDefCreate = new CustomSourceDefinitionCreate(
        new SourceDefinitionCreate(
            "Source E2E Test Connector",
            "airbyte/source-e2e-test",
            dockerImageTag,
            new URI("https://hub.docker.com/r/airbyte/source-e2e-test"),
            null,
            null),
        workspaceId,
        null,
        null);
    final SourceDefinitionRead srcDefRead = apiClient2.getSourceDefinitionApi().createCustomSourceDefinition(srcDefCreate);
    assertEquals(expectedProtocolVersion, srcDefRead.getProtocolVersion());

    final SourceDefinitionIdRequestBody srcDefReq = new SourceDefinitionIdRequestBody(srcDefRead.getSourceDefinitionId());
    final SourceDefinitionRead srcDefReadSanityCheck = apiClient2.getSourceDefinitionApi().getSourceDefinition(srcDefReq);
    assertEquals(srcDefRead.getProtocolVersion(), srcDefReadSanityCheck.getProtocolVersion());

    // Clean up the source
    apiClient2.getSourceDefinitionApi().deleteSourceDefinition(srcDefReq);
  }

  @ParameterizedTest
  @CsvSource({
    "2.1.1, 0.2.0",
    "2.1.2, 0.2.1",
  })
  void testCreateDestinationSpec(final String dockerImageTag, final String expectedProtocolVersion)
      throws URISyntaxException, IOException {
    final CustomDestinationDefinitionCreate dstDefCreate =
        new CustomDestinationDefinitionCreate(
            new DestinationDefinitionCreate(
                "Dest E2E Test Connector",
                "airbyte/source-e2e-test",
                dockerImageTag,
                new URI("https://hub.docker.com/r/airbyte/destination-e2e-test"),
                null,
                null),
            workspaceId,
            null,
            null);

    final DestinationDefinitionRead dstDefRead = apiClient2.getDestinationDefinitionApi().createCustomDestinationDefinition(dstDefCreate);
    assertEquals(expectedProtocolVersion, dstDefRead.getProtocolVersion());

    final DestinationDefinitionIdRequestBody dstDefReq = new DestinationDefinitionIdRequestBody(dstDefRead.getDestinationDefinitionId());
    final DestinationDefinitionRead dstDefReadSanityCheck = apiClient2.getDestinationDefinitionApi().getDestinationDefinition(dstDefReq);
    assertEquals(dstDefRead.getProtocolVersion(), dstDefReadSanityCheck.getProtocolVersion());

    // Clean up the destination
    apiClient2.getDestinationDefinitionApi().deleteDestinationDefinition(dstDefReq);
  }

}
