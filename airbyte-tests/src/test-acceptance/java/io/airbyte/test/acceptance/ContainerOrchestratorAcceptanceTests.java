/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.test.utils.AcceptanceTestHarness.waitForSuccessfulJob;
import static io.airbyte.test.utils.AcceptanceTestHarness.waitWhileJobHasStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.invoker.generated.ApiClient;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.DestinationDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.DestinationDefinitionRead;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.JobIdRequestBody;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobStatus;
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceDefinitionRead;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.test.utils.AcceptanceTestHarness;
import io.airbyte.test.utils.TestConnectionCreate;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This class tests behavior that is specific to container-orchestrator deployments, such as scaling
 * down and back up workers while a sync is running to ensure it is not affected by a deployment.
 * <p>
 * This test class is only enabled if the KUBE environment variable is true, because container
 * orchestrators are currently only used by kubernetes deployments, as container orchestrators have
 * not yet been ported over to docker.
 */
@SuppressWarnings({"ConstantConditions"})
@EnabledIfEnvironmentVariable(named = "KUBE",
                              matches = "true")
@TestInstance(Lifecycle.PER_CLASS)
class ContainerOrchestratorAcceptanceTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerOrchestratorAcceptanceTests.class);
  private static final String AIRBYTE_WORKER = Optional.ofNullable(System.getenv("AIRBYTE_WORKER_DEPLOYMENT_NAME")).orElse("airbyte-worker");
  private static final String NAMESPACE = Optional.ofNullable(System.getenv("NAMESPACE")).orElse("default");

  private static AcceptanceTestHarness testHarness;
  private static AirbyteApiClient apiClient;
  private static KubernetesClient kubernetesClient;

  private static final String AIRBYTE_SERVER_HOST = Optional.ofNullable(System.getenv("AIRBYTE_SERVER_HOST")).orElse("http://localhost:8001");

  @BeforeAll
  static void init() throws URISyntaxException, IOException, InterruptedException, ApiException {
    final URI url = new URI(AIRBYTE_SERVER_HOST);
    apiClient = new AirbyteApiClient(
        new ApiClient().setScheme(url.getScheme())
            .setHost(url.getHost())
            .setPort(url.getPort())
            .setBasePath("/api"));
    // work in whatever default workspace is present.
    final UUID workspaceId = apiClient.getWorkspaceApi().listWorkspaces().getWorkspaces().get(0).getWorkspaceId();
    LOGGER.info("workspaceId = " + workspaceId);

    // log which connectors are being used.
    final SourceDefinitionRead sourceDef = apiClient.getSourceDefinitionApi()
        .getSourceDefinition(new SourceDefinitionIdRequestBody()
            .sourceDefinitionId(UUID.fromString("decd338e-5647-4c0b-adf4-da0e75f5a750")));
    final DestinationDefinitionRead destinationDef = apiClient.getDestinationDefinitionApi()
        .getDestinationDefinition(new DestinationDefinitionIdRequestBody()
            .destinationDefinitionId(UUID.fromString("25c5221d-dce2-4163-ade9-739ef790f503")));
    LOGGER.info("pg source definition: {}", sourceDef.getDockerImageTag());
    LOGGER.info("pg destination definition: {}", destinationDef.getDockerImageTag());

    testHarness = new AcceptanceTestHarness(apiClient, workspaceId);
    kubernetesClient = testHarness.getKubernetesClient();
  }

  @AfterAll
  static void end() {
    testHarness.stopDbAndContainers();
  }

  @BeforeEach
  void setup() throws URISyntaxException, IOException, SQLException {
    testHarness.setup();
  }

  // This test is flaky. Warnings are suppressed until that condition us understood
  // See: https://github.com/airbytehq/airbyte/issues/19948
  @Test
  @Disabled("Flaky test, to be investigated before re-enabling")
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void testDowntimeDuringSync() throws Exception {
    // NOTE: PMD assert warning suppressed because the assertion was flaky. The test will throw if the
    // sync does not succeed.
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();
    final SyncMode syncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode destinationSyncMode = DestinationSyncMode.OVERWRITE;
    catalog.getStreams().forEach(s -> s.getConfig().syncMode(syncMode).destinationSyncMode(destinationSyncMode));

    LOGGER.info("Creating connection...");
    final UUID connectionId =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .build())
            .getConnectionId();

    LOGGER.info("Run manual sync...");
    final JobInfoRead connectionSyncRead = apiClient.getConnectionApi().syncConnection(new ConnectionIdRequestBody().connectionId(connectionId));

    LOGGER.info("Waiting for job to run...");
    waitWhileJobHasStatus(apiClient.getJobsApi(), connectionSyncRead.getJob(), Set.of(JobStatus.PENDING));

    LOGGER.info("Scaling down worker...");
    kubernetesClient.apps().deployments().inNamespace(NAMESPACE).withName(AIRBYTE_WORKER).scale(0, true);

    LOGGER.info("Scaling up worker...");
    kubernetesClient.apps().deployments().inNamespace(NAMESPACE).withName(AIRBYTE_WORKER).scale(1);

    waitForSuccessfulJob(apiClient.getJobsApi(), connectionSyncRead.getJob());
  }

  @AfterEach
  void tearDown() {
    testHarness.cleanup();
  }

  @Test
  void testCancelSyncWithInterruption() throws Exception {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();
    final SyncMode syncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode destinationSyncMode = DestinationSyncMode.OVERWRITE;
    catalog.getStreams().forEach(s -> s.getConfig().syncMode(syncMode).destinationSyncMode(destinationSyncMode));
    final UUID connectionId =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .build())
            .getConnectionId();

    final JobInfoRead connectionSyncRead = apiClient.getConnectionApi().syncConnection(new ConnectionIdRequestBody().connectionId(connectionId));
    waitWhileJobHasStatus(apiClient.getJobsApi(), connectionSyncRead.getJob(), Set.of(JobStatus.PENDING));

    kubernetesClient.apps().deployments().inNamespace(NAMESPACE).withName(AIRBYTE_WORKER).scale(0, true);
    kubernetesClient.apps().deployments().inNamespace(NAMESPACE).withName(AIRBYTE_WORKER).scale(1);

    final var resp = apiClient.getJobsApi().cancelJob(new JobIdRequestBody().id(connectionSyncRead.getJob().getId()));
    assertEquals(JobStatus.CANCELLED, resp.getJob().getStatus());
  }

  @Test
  void testCancelSyncWhenCancelledWhenWorkerIsNotRunning() throws Exception {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();
    final SyncMode syncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode destinationSyncMode = DestinationSyncMode.OVERWRITE;
    catalog.getStreams().forEach(s -> s.getConfig().syncMode(syncMode).destinationSyncMode(destinationSyncMode));

    LOGGER.info("Creating connection...");
    final UUID connectionId =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .build())
            .getConnectionId();

    LOGGER.info("Waiting for connection to be available in Temporal...");

    LOGGER.info("Run manual sync...");
    final JobInfoRead connectionSyncRead = apiClient.getConnectionApi().syncConnection(new ConnectionIdRequestBody().connectionId(connectionId));

    LOGGER.info("Waiting for job to run...");
    waitWhileJobHasStatus(apiClient.getJobsApi(), connectionSyncRead.getJob(), Set.of(JobStatus.PENDING));

    LOGGER.info("Waiting for job to run a little...");
    Thread.sleep(1000);

    LOGGER.info("Scale down workers...");
    kubernetesClient.apps().deployments().inNamespace(NAMESPACE).withName(AIRBYTE_WORKER).scale(0, true);

    LOGGER.info("Starting background cancellation request...");
    final var pool = Executors.newSingleThreadExecutor();
    final var mdc = MDC.getCopyOfContextMap();
    final Future<JobInfoRead> resp =
        pool.submit(() -> {
          MDC.setContextMap(mdc);
          try {
            final JobInfoRead jobInfoRead = apiClient.getJobsApi().cancelJob(new JobIdRequestBody().id(connectionSyncRead.getJob().getId()));
            LOGGER.info("jobInfoRead = " + jobInfoRead);
            return jobInfoRead;
          } catch (final ApiException e) {
            LOGGER.error("Failed to read from api", e);
            throw e;
          }
        });
    Thread.sleep(2000);

    LOGGER.info("Scaling up workers...");
    kubernetesClient.apps().deployments().inNamespace(NAMESPACE).withName(AIRBYTE_WORKER).scale(1);

    LOGGER.info("Waiting for cancellation to go into effect...");
    assertEquals(JobStatus.CANCELLED, resp.get().getJob().getStatus());
  }

}
