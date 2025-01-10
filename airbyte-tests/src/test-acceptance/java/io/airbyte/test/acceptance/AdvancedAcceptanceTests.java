/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.test.utils.AcceptanceTestHarness.PUBLIC_SCHEMA_NAME;
import static io.airbyte.test.utils.AcceptanceTestUtils.createAirbyteApiClient;
import static io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog;

import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.DestinationDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.DestinationDefinitionRead;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceDefinitionRead;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.StreamStatusJobType;
import io.airbyte.api.client.model.generated.StreamStatusRunState;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.test.utils.AcceptanceTestHarness;
import io.airbyte.test.utils.Asserts;
import io.airbyte.test.utils.TestConnectionCreate;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.junitpioneer.jupiter.RetryingTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class test for advanced platform functionality that can be affected by the networking
 * difference between the Kube and Docker deployments i.e. distributed vs local processes. All tests
 * in this class should pass when ran on either type of deployment.
 * <p>
 * Tests use the {@link RetryingTest} annotation instead of the more common {@link Test} to allow
 * multiple tries for a test to pass. This is because these tests sometimes fail transiently, and we
 * haven't been able to fix that yet.
 * <p>
 * However, in general we should prefer using {@code @Test} instead and only resort to using
 * {@code @RetryingTest} for tests that we can't get to pass reliably. New tests should thus default
 * to using {@code @Test} if possible.
 * <p>
 * We order tests such that earlier tests cover more basic behavior that is relied upon in later
 * tests. e.g. We test that we can create a destination before we test whether we can sync data to
 * it.
 */
@SuppressWarnings({"ConstantConditions"})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(Lifecycle.PER_CLASS)
@Tags({@Tag("sync"), @Tag("enterprise")})
class AdvancedAcceptanceTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedAcceptanceTests.class);

  private static AcceptanceTestHarness testHarness;
  private static UUID workspaceId;
  private static final String AIRBYTE_SERVER_HOST = Optional.ofNullable(System.getenv("AIRBYTE_SERVER_HOST")).orElse("http://localhost:8001");

  @BeforeAll
  static void init() throws Exception {
    final var apiClient = createAirbyteApiClient(AIRBYTE_SERVER_HOST + "/api", Map.of());

    // work in whatever default workspace is present.
    workspaceId = apiClient.getWorkspaceApi().listWorkspaces().getWorkspaces().getFirst().getWorkspaceId();
    LOGGER.info("workspaceId = {}", workspaceId);

    // log which connectors are being used.
    final SourceDefinitionRead sourceDef = apiClient.getSourceDefinitionApi()
        .getSourceDefinition(new SourceDefinitionIdRequestBody(UUID.fromString("decd338e-5647-4c0b-adf4-da0e75f5a750")));
    final DestinationDefinitionRead destinationDef = apiClient.getDestinationDefinitionApi()
        .getDestinationDefinition(new DestinationDefinitionIdRequestBody(UUID.fromString("25c5221d-dce2-4163-ade9-739ef790f503")));
    LOGGER.info("pg source definition: {}", sourceDef.getDockerImageTag());
    LOGGER.info("pg destination definition: {}", destinationDef.getDockerImageTag());

    testHarness = new AcceptanceTestHarness(apiClient, workspaceId);
  }

  @AfterAll
  static void end() {
    testHarness.stopDbAndContainers();
  }

  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  @Test
  void testManualSync() throws Exception {
    testHarness.setup();

    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();

    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final SyncMode syncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode destinationSyncMode = DestinationSyncMode.OVERWRITE;
    final AirbyteCatalog catalog = modifyCatalog(
        discoverResult.getCatalog(),
        Optional.of(syncMode),
        Optional.of(destinationSyncMode),
        Optional.empty(),
        Optional.empty(),
        Optional.of(true),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    final var conn =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .build());
    final var connectionId = conn.getConnectionId();
    final JobInfoRead connectionSyncRead = testHarness.syncConnection(connectionId);
    testHarness.waitForSuccessfulJob(connectionSyncRead.getJob());
    Asserts.assertSourceAndDestinationDbRawRecordsInSync(testHarness.getSourceDatabase(), testHarness.getDestinationDatabase(), PUBLIC_SCHEMA_NAME,
        conn.getNamespaceFormat(), false, false);

    Asserts.assertStreamStatuses(testHarness, workspaceId, connectionId, connectionSyncRead.getJob().getId(), StreamStatusRunState.COMPLETE,
        StreamStatusJobType.SYNC);

    testHarness.cleanup();
  }

}
