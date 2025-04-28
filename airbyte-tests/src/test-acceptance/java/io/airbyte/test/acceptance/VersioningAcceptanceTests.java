/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.test.utils.AcceptanceTestUtils.createAirbyteApiClient;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.CustomDestinationDefinitionCreate;
import io.airbyte.api.client.model.generated.CustomSourceDefinitionCreate;
import io.airbyte.api.client.model.generated.DestinationDefinitionCreate;
import io.airbyte.api.client.model.generated.DestinationDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.DestinationDefinitionRead;
import io.airbyte.api.client.model.generated.SourceDefinitionCreate;
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceDefinitionRead;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@TestInstance(Lifecycle.PER_CLASS)
@Tags({@Tag("sync"), @Tag("enterprise")})
class VersioningAcceptanceTests {

  private static AirbyteApiClient apiClient2;
  private static UUID workspaceId;
  private static final String AIRBYTE_SERVER_HOST = Optional.ofNullable(System.getenv("AIRBYTE_SERVER_HOST")).orElse("http://localhost:8001");

  @BeforeAll
  static void init() throws IOException, GeneralSecurityException, URISyntaxException, InterruptedException {
    apiClient2 = createAirbyteApiClient(String.format("%s/api", AIRBYTE_SERVER_HOST), Map.of());

    AcceptanceTestsResources acceptanceTestsResources = new AcceptanceTestsResources();
    acceptanceTestsResources.init();
    workspaceId = acceptanceTestsResources.getWorkspaceId();
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
