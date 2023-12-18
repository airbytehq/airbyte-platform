/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static com.airbyte.api.models.shared.SourcePostgresPostgres.POSTGRES;
import static io.airbyte.test.acceptance.BasicAcceptanceTests.FINAL_INTERVAL_SECS;
import static io.airbyte.test.acceptance.BasicAcceptanceTests.IS_GKE;
import static io.airbyte.test.acceptance.BasicAcceptanceTests.JITTER_MAX_INTERVAL_SECS;
import static io.airbyte.test.acceptance.BasicAcceptanceTests.MAX_TRIES;
import static io.airbyte.test.acceptance.BasicAcceptanceTests.TRUE;
import static io.airbyte.test.utils.AcceptanceTestHarness.PUBLIC_SCHEMA_NAME;
import static io.airbyte.test.utils.AcceptanceTestHarness.waitForSuccessfulJob;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.airbyte.api.Airbyte;
import com.airbyte.api.Connections;
import com.airbyte.api.Destinations;
import com.airbyte.api.Jobs;
import com.airbyte.api.Sources;
import com.airbyte.api.Workspaces;
import com.airbyte.api.models.operations.CreateWorkspaceResponse;
import com.airbyte.api.models.operations.ListConnectionsRequest;
import com.airbyte.api.models.operations.ListConnectionsResponse;
import com.airbyte.api.models.operations.ListDestinationsRequest;
import com.airbyte.api.models.operations.ListJobsRequest;
import com.airbyte.api.models.operations.ListJobsResponse;
import com.airbyte.api.models.operations.ListSourcesRequest;
import com.airbyte.api.models.operations.ListWorkspacesRequest;
import com.airbyte.api.models.operations.PatchConnectionRequest;
import com.airbyte.api.models.operations.PatchDestinationRequest;
import com.airbyte.api.models.operations.PatchSourceRequest;
import com.airbyte.api.models.operations.PatchSourceResponse;
import com.airbyte.api.models.shared.ConnectionPatchRequest;
import com.airbyte.api.models.shared.ConnectionResponse;
import com.airbyte.api.models.shared.ConnectionStatusEnum;
import com.airbyte.api.models.shared.DestinationPatchRequest;
import com.airbyte.api.models.shared.DestinationResponse;
import com.airbyte.api.models.shared.JobCreateRequest;
import com.airbyte.api.models.shared.JobResponse;
import com.airbyte.api.models.shared.JobStatusEnum;
import com.airbyte.api.models.shared.JobTypeEnum;
import com.airbyte.api.models.shared.SchemeBasicAuth;
import com.airbyte.api.models.shared.Security;
import com.airbyte.api.models.shared.SourcePatchRequest;
import com.airbyte.api.models.shared.SourcePostgresPostgres;
import com.airbyte.api.models.shared.SourceResponse;
import com.airbyte.api.models.shared.WorkspaceCreateRequest;
import com.airbyte.api.models.shared.WorkspaceResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.invoker.generated.ApiClient;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.api.client.model.generated.DestinationRead;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.JobConfigType;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobListForWorkspacesRequestBody;
import io.airbyte.api.client.model.generated.JobWithAttemptsRead;
import io.airbyte.api.client.model.generated.ListResourcesForWorkspacesRequestBody;
import io.airbyte.api.client.model.generated.Pagination;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SourceRead;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.api.client.model.generated.WorkspaceCreate;
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.test.utils.AcceptanceTestHarness;
import io.airbyte.test.utils.Asserts;
import io.airbyte.test.utils.TestConnectionCreate;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains tests that run against the Airbyte API.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(Lifecycle.PER_CLASS)
@SuppressWarnings("PMD")
public class AirbyteApiAcceptanceTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(AirbyteApiAcceptanceTests.class);
  private static Airbyte airbyteApiClient;
  private static AirbyteApiClient configApiClient;
  private static AcceptanceTestHarness testHarness;

  private static final String GATEWAY_AUTH_HEADER = "X-Endpoint-API-UserInfo";
  // NOTE: this is just a base64 encoding of a jwt representing a test user in some deployments.
  private static final String AIRBYTE_AUTH_HEADER = "eyJ1c2VyX2lkIjogImNsb3VkLWFwaSIsICJlbWFpbF92ZXJpZmllZCI6ICJ0cnVlIn0K";
  private static final String AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID = "AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID";
  private static final String AIRBYTE_SERVER_HOST = Optional.ofNullable(System.getenv("AIRBYTE_SERVER_HOST")).orElse("http://localhost:8001");
  private static final String AIRBYTE_PUBLIC_API_SERVER_HOST =
      Optional.ofNullable(System.getenv("AIRBYTE_PUBLIC_API_SERVER_HOST")).orElse("http://localhost:8006");
  private static final String LOCAL_PUBLIC_API_SERVER_URL = "http://localhost:8006/v1";
  private static final String AUTHORIZATION_HEADER = "AUTHORIZATION";
  // NOTE: this is just the default airbyte/password user's basic auth header.
  private static final String AIRBYTE_BASIC_AUTH_HEADER = "Basic YWlyYnl0ZTpwYXNzd29yZA==";

  private static UUID workspaceId;

  private static final String TEST_ACTOR_NAME = "test-actor-name";

  private static Jobs airbyteApiJobsClient;
  private static JobsApi configApiJobsClient;
  private static Connections airbyteApiConnectionsClient;
  private static ConnectionApi configApiConnectionsClient;
  private static Workspaces airbyteApiWorkspacesClient;
  private static Sources airbyteApiSourcesClient;
  private static Destinations airbyteApiDestinationsClient;

  @BeforeAll
  static void init() throws Exception {

    final boolean isGke = System.getenv().containsKey(IS_GKE);
    // Set up the API client.
    final URI url = new URI(AIRBYTE_SERVER_HOST);
    final var underlyingApiClient = new ApiClient().setScheme(url.getScheme())
        .setHost(url.getHost())
        .setPort(url.getPort())
        .setBasePath("/api");
    underlyingApiClient.setRequestInterceptor(builder -> builder.setHeader(AUTHORIZATION_HEADER, AIRBYTE_BASIC_AUTH_HEADER));
    if (isGke) {
      underlyingApiClient.setRequestInterceptor(builder -> builder.setHeader(GATEWAY_AUTH_HEADER, AIRBYTE_AUTH_HEADER));
    }
    configApiClient = new AirbyteApiClient(underlyingApiClient);

    // If a workspace id is passed, use that. Otherwise, create a new workspace.
    // NOTE: we want to sometimes use a pre-configured workspace e.g., if we run against a production
    // deployment where we don't want to create workspaces.
    // NOTE: the API client can't create workspaces in GKE deployments, so we need to provide a
    // workspace ID in that environment.
    workspaceId = System.getenv().get(AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID) == null ? configApiClient.getWorkspaceApi()
        .createWorkspace(new WorkspaceCreate().email("acceptance-tests@airbyte.io").name("Airbyte Acceptance Tests" + UUID.randomUUID()))
        .getWorkspaceId()
        : UUID.fromString(System.getenv().get(AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID));
    LOGGER.info("workspaceId = " + workspaceId);

    testHarness = new AcceptanceTestHarness(configApiClient, workspaceId);

    testHarness.ensureCleanSlate();
    final URI publicApiUrl = new URI(AIRBYTE_PUBLIC_API_SERVER_HOST);
    airbyteApiClient = Airbyte.builder()
        .setServerURL(publicApiUrl.toString())
        .setSecurity(new Security() {

          {
            new SchemeBasicAuth("airbyte", "password");
          }

        })
        .build();

    airbyteApiJobsClient = airbyteApiClient.jobs;
    configApiJobsClient = configApiClient.getJobsApi();

    airbyteApiWorkspacesClient = airbyteApiClient.workspaces;
    airbyteApiSourcesClient = airbyteApiClient.sources;
    airbyteApiDestinationsClient = airbyteApiClient.destinations;

    airbyteApiConnectionsClient = airbyteApiClient.connections;
    configApiConnectionsClient = configApiClient.getConnectionApi();
    testHarness.ensureCleanSlate();
  }

  @AfterAll
  static void end() {
    LOGGER.debug("Executing test suite teardown");
    testHarness.stopDbAndContainers();
  }

  @BeforeEach
  void setup() throws SQLException, URISyntaxException, IOException {
    LOGGER.debug("Executing test case setup");
    testHarness.setup();
  }

  @AfterEach
  void tearDown() {
    LOGGER.debug("Executing test case teardown");
    testHarness.cleanup();
  }

  @Disabled
  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE)
  @Timeout(value = 10,
           unit = TimeUnit.MINUTES)
  void testJobsEndpointThroughPublicAPI() throws Exception {
    testHarness.setup();

    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();
    final SyncMode syncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode destinationSyncMode = DestinationSyncMode.OVERWRITE;
    catalog.getStreams().forEach(s -> s.getConfig().syncMode(syncMode).selected(true).destinationSyncMode(destinationSyncMode));
    final var conn =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .build());
    final var connectionId = conn.getConnectionId();
    final JobTypeEnum expectedJobType = JobTypeEnum.SYNC;

    // first sync and list jobs
    final JobCreateRequest jobCreate = new JobCreateRequest(
        connectionId.toString(),
        expectedJobType);
    jobCreate.withConnectionId(connectionId.toString());
    jobCreate.withJobType(expectedJobType);
    airbyteApiClient.jobs.createJob(jobCreate);
    waitForSuccessfulJob(configApiClient.getJobsApi(), testHarness.getMostRecentSyncForConnection(connectionId));

    // Test regular old get jobs
    final ListJobsRequest listJobsRequest = new ListJobsRequest()
        .withConnectionId(connectionId.toString())
        .withLimit(1000)
        .withOffset(0)
        .withJobType(expectedJobType);
    ListJobsResponse jobsApiResponse =
        airbyteApiClient.jobs.listJobs(listJobsRequest);
    assertEquals(200, jobsApiResponse.statusCode);
    List<JobResponse> actualJobsResponse = Arrays.stream(jobsApiResponse.jobsResponse.data).toList();
    assertEquals(1, actualJobsResponse.size());
    JobResponse actualJobResponse = actualJobsResponse.get(0);
    assertNotNull(actualJobResponse.jobId);
    assertEquals(expectedJobType, actualJobResponse.jobType);
    assertEquals(JobStatusEnum.SUCCEEDED, actualJobResponse.status);

    // Test filter status
    final ListJobsRequest filterStatusRequest = new ListJobsRequest()
        .withConnectionId(connectionId.toString())
        .withLimit(5)
        .withOffset(0)
        .withJobType(expectedJobType)
        .withStatus(JobStatusEnum.CANCELLED);
    jobsApiResponse = airbyteApiJobsClient.listJobs(filterStatusRequest);

    assertEquals(200, jobsApiResponse.statusCode);
    assertNull(jobsApiResponse.jobsResponse.data);

    filterStatusRequest.withStatus(JobStatusEnum.SUCCEEDED);
    jobsApiResponse = airbyteApiJobsClient.listJobs(filterStatusRequest);

    assertEquals(200, jobsApiResponse.statusCode);
    actualJobsResponse = Arrays.stream(jobsApiResponse.jobsResponse.data).toList();
    assertEquals(1, actualJobsResponse.size());
    actualJobResponse = actualJobsResponse.get(0);
    assertNotNull(actualJobResponse.jobId);
    assertEquals(expectedJobType, actualJobResponse.jobType);
    assertEquals(JobStatusEnum.SUCCEEDED, actualJobResponse.status);

    // Filter by time
    // final ListJobsRequest filterTimeRequest = new ListJobsRequest()
    // .withConnectionId(connectionId.toString())
    // .withLimit(5)
    // .withOffset(0)
    // .withJobType(expectedJobType)
    // .withCreatedAtStart(OffsetDateTime.now(ZoneOffset.UTC));
    // jobsApiResponse = airbyteApiJobsClient.listJobs(filterTimeRequest);
    // assertEquals(200, jobsApiResponse.statusCode);
    // actualJobsResponse = Arrays.stream(jobsApiResponse.jobsResponse.data).toList();
    // assertEquals("", actualJobsResponse.get(0).jobId);
    // assertEquals(0, actualJobsResponse.size());
    //
    // filterTimeRequest.withCreatedAtStart(OffsetDateTime.of(LocalDateTime.of(2023, 1, 1, 0, 0),
    // ZoneOffset.UTC));
    // jobsApiResponse = airbyteApiJobsClient.listJobs(filterStatusRequest);
    // assertEquals(200, jobsApiResponse.statusCode);
    // actualJobsResponse = Arrays.stream(jobsApiResponse.jobsResponse.data).toList();
    // assertEquals(1, actualJobsResponse.size());
    // actualJobResponse = actualJobsResponse.get(0);
    // assertNotNull(actualJobResponse.jobId);
    // assertEquals(expectedJobType, actualJobResponse.jobType);
    // assertEquals(JobStatusEnum.SUCCEEDED, actualJobResponse.status);

    Asserts.assertSourceAndDestinationDbRawRecordsInSync(testHarness.getSourceDatabase(), testHarness.getDestinationDatabase(), PUBLIC_SCHEMA_NAME,
        conn.getNamespaceFormat(),
        false, false);

    // List
    listJobsRequest.withConnectionId(null);
    jobsApiResponse = airbyteApiJobsClient.listJobs(listJobsRequest);
    assertEquals(200, jobsApiResponse.statusCode);
    final List<UUID> allWorkspaceIds = Arrays.stream(
        airbyteApiWorkspacesClient.listWorkspaces(new ListWorkspacesRequest().withLimit(1000)).workspacesResponse.data)
        .map(workspaceResponse -> UUID.fromString(workspaceResponse.workspaceId)).toList();
    final List<JobWithAttemptsRead> configApiJobsList = configApiJobsClient.listJobsForWorkspaces(
        new JobListForWorkspacesRequestBody()
            .workspaceIds(allWorkspaceIds)
            .addConfigTypesItem(JobConfigType.SYNC))
        .getJobs();
    actualJobsResponse = Arrays.stream(jobsApiResponse.jobsResponse.data).toList();
    assertEquals(configApiJobsList.size(), actualJobsResponse.size());

    // then reset and list jobs
    // expectedJobType = JobTypeEnum.RESET;
    // The lines below are commented out, and we use the adminApiClient because the test harness does
    // not currently have a getMostRecentResetJobId method
    // jobCreate.setJobType(expectedJobType);
    // publicApiJobsClient.createJob(jobCreate);

    final JobInfoRead jobInfoRead = configApiClient.getConnectionApi().resetConnection(new ConnectionIdRequestBody().connectionId(connectionId));
    waitForSuccessfulJob(configApiJobsClient, jobInfoRead.getJob());

    // null response type means we return both sync and reset
    final ListJobsRequest noFilterRequest = new ListJobsRequest().withLimit(1000);
    jobsApiResponse = airbyteApiJobsClient.listJobs(noFilterRequest);
    assertEquals(200, jobsApiResponse.statusCode);
    final List<JobWithAttemptsRead> configApiJobsFullList = configApiJobsClient.listJobsForWorkspaces(
        new JobListForWorkspacesRequestBody().configTypes(List.of(JobConfigType.SYNC, JobConfigType.RESET_CONNECTION))
            .workspaceIds(allWorkspaceIds))
        .getJobs();
    actualJobsResponse = Arrays.stream(jobsApiResponse.jobsResponse.data).toList();
    assertEquals(configApiJobsFullList.size(), actualJobsResponse.size());
  }

  @Disabled
  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE)
  @Timeout(value = 10,
           unit = TimeUnit.MINUTES)
  void testListConnectionsThroughPublicAPI() throws Exception {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();
    final SyncMode syncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode destinationSyncMode = DestinationSyncMode.OVERWRITE;
    catalog.getStreams().forEach(s -> s.getConfig().syncMode(syncMode).selected(true).destinationSyncMode(destinationSyncMode));

    final UUID connectionOne =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .build())
            .getConnectionId();
    final UUID connectionTwo =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .build())
            .getConnectionId();

    final List<ConnectionRead> expectedConnections = configApiClient.getConnectionApi().listAllConnectionsForWorkspace(
        new WorkspaceIdRequestBody().workspaceId(workspaceId)).getConnections();
    final List<UUID> expectedNonDeletedConnections = expectedConnections
        .stream()
        .filter((connection) -> !connection.getStatus().equals(ConnectionStatus.DEPRECATED))
        .map(ConnectionRead::getConnectionId)
        .sorted()
        .toList();

    final ListConnectionsRequest listConnectionsRequest = new ListConnectionsRequest()
        .withWorkspaceIds(new String[] {workspaceId.toString()})
        .withIncludeDeleted(false);
    final ListConnectionsResponse connectionResponsesExcludingDeleted =
        airbyteApiConnectionsClient.listConnections(listConnectionsRequest);

    final List<ConnectionResponse> connectionResponseList = Arrays.asList(connectionResponsesExcludingDeleted.connectionsResponse.data);
    assertEquals(
        expectedNonDeletedConnections,
        connectionResponseList.stream().map(connection -> UUID.fromString(connection.connectionId)).sorted().toList());
    assertTrue(
        connectionResponseList.stream()
            .map(connection -> UUID.fromString(connection.connectionId))
            .toList()
            .containsAll(List.of(connectionOne, connectionTwo)));

    final WorkspaceCreateRequest workspaceCreateRequest = new WorkspaceCreateRequest("new-workspace");
    final CreateWorkspaceResponse workspaceResponse = airbyteApiWorkspacesClient.createWorkspace(workspaceCreateRequest);
    final UUID newWorkspaceId = UUID.fromString(workspaceResponse.workspaceResponse.workspaceId);
    final UUID sourceThree = testHarness.createSource(
        "acceptanceTestDb-" + UUID.randomUUID(),
        newWorkspaceId,
        testHarness.getPostgresSourceDefinitionId(),
        testHarness.getSourceDbConfig()).getSourceId();

    final UUID destinationThree = testHarness.createDestination(
        "AccTestDestination-" + UUID.randomUUID(),
        newWorkspaceId,
        testHarness.getPostgresDestinationDefinitionId(),
        testHarness.getDestinationDbConfig()).getDestinationId();
    final UUID connectionThree =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceThree,
            destinationThree,
            catalog,
            discoverResult.getCatalogId())
                .build())
            .getConnectionId();

    final ListConnectionsRequest listConnectionsRequestNewWorkspace = new ListConnectionsRequest()
        .withWorkspaceIds(new String[] {newWorkspaceId.toString()})
        .withIncludeDeleted(false);

    final List<ConnectionResponse> connectionResponsesNewWorkspace =
        Arrays.asList(airbyteApiConnectionsClient.listConnections(listConnectionsRequestNewWorkspace).connectionsResponse.data);
    final List<UUID> resultingConnections = connectionResponsesNewWorkspace
        .stream().map(connection -> UUID.fromString(connection.connectionId)).toList();
    assertEquals(List.of(connectionThree), resultingConnections);

    final ListConnectionsRequest listConnectionsRequestNoPage = new ListConnectionsRequest()
        .withWorkspaceIds(new String[] {newWorkspaceId.toString()})
        .withIncludeDeleted(false)
        .withLimit(0);
    final ConnectionResponse[] connectionResponsesNewWorkspaceNoPage =
        airbyteApiConnectionsClient.listConnections(listConnectionsRequestNoPage).connectionsResponse.data;
    assertNull(connectionResponsesNewWorkspaceNoPage);

    final ListConnectionsRequest listConnectionsRequestOffset = new ListConnectionsRequest()
        .withWorkspaceIds(new String[] {newWorkspaceId.toString()})
        .withIncludeDeleted(false)
        .withLimit(10)
        .withOffset(1);
    final ConnectionResponse[] connectionResponsesNewWorkspaceOffset =
        airbyteApiConnectionsClient.listConnections(listConnectionsRequestOffset).connectionsResponse.data;
    assertNull(connectionResponsesNewWorkspaceOffset);
  }

  @Disabled
  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE)
  @Timeout(value = 10,
           unit = TimeUnit.MINUTES)
  void testCreateAndListSourcesThroughPublicAPI() throws Exception {
    final WorkspaceCreateRequest workspaceCreateRequest = new WorkspaceCreateRequest("new-workspace");
    final CreateWorkspaceResponse workspaceResponse = airbyteApiWorkspacesClient.createWorkspace(workspaceCreateRequest);

    final UUID newWorkspaceId = UUID.fromString(workspaceResponse.workspaceResponse.workspaceId);

    testHarness.createSource(
        "listSourcesTest" + UUID.randomUUID(),
        newWorkspaceId,
        testHarness.getPostgresSourceDefinitionId(),
        testHarness.getSourceDbConfig());

    testHarness.createSource(
        "listSourcesTest" + UUID.randomUUID(),
        newWorkspaceId,
        testHarness.getPostgresSourceDefinitionId(),
        testHarness.getSourceDbConfig());

    final List<SourceRead> expectedSources = configApiClient.getSourceApi().listSourcesForWorkspace(
        new WorkspaceIdRequestBody().workspaceId(newWorkspaceId)).getSources();
    final List<UUID> expectedSourceIds = expectedSources
        .stream()
        .map(SourceRead::getSourceId)
        .sorted()
        .toList();

    final ListSourcesRequest listSourcesRequest = new ListSourcesRequest()
        .withWorkspaceIds(new String[] {newWorkspaceId.toString()})
        .withIncludeDeleted(false);
    final List<UUID> sourceResponseList =
        Arrays.stream(airbyteApiSourcesClient.listSources(listSourcesRequest).sourcesResponse.data)
            .map(sourceResponse -> UUID.fromString(sourceResponse.sourceId)).sorted().toList();
    assertTrue(sourceResponseList.containsAll(expectedSourceIds));

    // Confirm that when we list all sources from the public API, we get the same results when listing
    // via the config API
    final ListSourcesRequest listSourcesRequestNoWorkspaceFilter = new ListSourcesRequest()
        .withLimit(1000)
        .withIncludeDeleted(false);
    final List<SourceResponse> sourceResponseListNullWorkspaceIds =
        Arrays.stream(airbyteApiSourcesClient.listSources(listSourcesRequestNoWorkspaceFilter).sourcesResponse.data)
            .toList();
    final List<UUID> sourceResponseListNullWorkspaceIdsSourceIds = sourceResponseListNullWorkspaceIds.stream()
        .map(sourceResponse -> UUID.fromString(sourceResponse.sourceId))
        .sorted()
        .toList();
    final List<SourceRead> expectedAllSources = configApiClient.getSourceApi()
        .listSourcesForWorkspacePaginated(
            new ListResourcesForWorkspacesRequestBody().workspaceIds(
                sourceResponseListNullWorkspaceIds.stream().map(sourceResponse -> UUID.fromString(sourceResponse.workspaceId)).toList())
                .pagination(new Pagination().pageSize(1000).rowOffset(0)))
        .getSources();
    final List<UUID> expectedAllSourceIds = expectedAllSources
        .stream()
        .map(SourceRead::getSourceId)
        .sorted()
        .toList();
    // Since we retry the createSource api call, it is possible for more sources than expected to be
    // created. Assert a containsAll instead of equals.
    assertEquals(expectedAllSourceIds, sourceResponseListNullWorkspaceIdsSourceIds);
    assertTrue(sourceResponseListNullWorkspaceIdsSourceIds.containsAll(expectedSourceIds));

    final ListSourcesRequest listSourcesRequestNoPage = new ListSourcesRequest()
        .withWorkspaceIds(new String[] {newWorkspaceId.toString()})
        .withLimit(0)
        .withIncludeDeleted(false);
    final SourceResponse[] sourceResponsesNoPage =
        airbyteApiSourcesClient.listSources(listSourcesRequestNoPage).sourcesResponse.data;
    assertNull(sourceResponsesNoPage);

    final ListSourcesRequest listSourcesRequestOffset = new ListSourcesRequest()
        .withWorkspaceIds(new String[] {newWorkspaceId.toString()})
        .withOffset(1)
        .withIncludeDeleted(false);
    final List<SourceResponse> sourceResponsesOffset =
        Arrays.asList(airbyteApiSourcesClient.listSources(listSourcesRequestOffset).sourcesResponse.data);
    assertEquals(1, sourceResponsesOffset.size());

  }

  @Disabled
  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE)
  @Timeout(value = 10,
           unit = TimeUnit.MINUTES)
  void testCreateAndListWorkspacesThroughPublicAPi() throws Exception {

    final WorkspaceCreateRequest workspaceCreateRequest = new WorkspaceCreateRequest("workspaces-workspace");
    final UUID workspaceOne = UUID.fromString(airbyteApiWorkspacesClient.createWorkspace(workspaceCreateRequest).workspaceResponse.workspaceId);
    final UUID workspaceTwo = UUID.fromString(airbyteApiWorkspacesClient.createWorkspace(workspaceCreateRequest).workspaceResponse.workspaceId);

    final List<UUID> expectedWorkspaceIds = Stream.of(workspaceOne, workspaceTwo).sorted().toList();

    final ListWorkspacesRequest listWorkspacesRequest = new ListWorkspacesRequest()
        .withWorkspaceIds(new String[] {String.valueOf(workspaceOne), String.valueOf(workspaceTwo)})
        .withIncludeDeleted(false);
    final List<WorkspaceResponse> workspaceReadList =
        Arrays.asList(airbyteApiWorkspacesClient.listWorkspaces(listWorkspacesRequest).workspacesResponse.data);

    assertEquals(expectedWorkspaceIds, workspaceReadList.stream()
        .map(workspaceResponse -> UUID.fromString(workspaceResponse.workspaceId)).sorted().toList());

    final ListWorkspacesRequest listAllWorkspacesRequest = new ListWorkspacesRequest()
        .withIncludeDeleted(false)
        .withLimit(1000);
    final List<WorkspaceResponse> workspaceReadListNullWorkspaceIds =
        Arrays.asList(airbyteApiWorkspacesClient.listWorkspaces(listAllWorkspacesRequest).workspacesResponse.data);

    final List<UUID> allExpectedWorkspaceIds =
        configApiClient.getWorkspaceApi()
            .listAllWorkspacesPaginated(
                new ListResourcesForWorkspacesRequestBody()
                    .includeDeleted(false)
                    .pagination(new Pagination().pageSize(1000).rowOffset(0)))
            .getWorkspaces()
            .stream()
            .map(WorkspaceRead::getWorkspaceId)
            .sorted()
            .toList();

    assertEquals(allExpectedWorkspaceIds, workspaceReadListNullWorkspaceIds.stream()
        .map(workspaceResponse -> UUID.fromString(workspaceResponse.workspaceId)).sorted().toList());

    final ListWorkspacesRequest listWorkspacesRequestNoPage = new ListWorkspacesRequest()
        .withIncludeDeleted(false)
        .withLimit(0);
    final WorkspaceResponse[] workspaceResponsesNoPage =
        airbyteApiWorkspacesClient.listWorkspaces(listWorkspacesRequestNoPage).workspacesResponse.data;
    assertNull(workspaceResponsesNoPage);

    final ListWorkspacesRequest listWorkspacesRequestOffset = new ListWorkspacesRequest()
        .withWorkspaceIds(new String[] {String.valueOf(workspaceOne), String.valueOf(workspaceTwo)})
        .withIncludeDeleted(false)
        .withOffset(1);
    final List<WorkspaceResponse> workspaceResponsesOffset =
        Arrays.asList(airbyteApiWorkspacesClient.listWorkspaces(listWorkspacesRequestOffset).workspacesResponse.data);
    assertEquals(1, workspaceResponsesOffset.size());
  }

  @Disabled
  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE)
  @Timeout(value = 10,
           unit = TimeUnit.MINUTES)
  void testCreateAndListDestinationsThroughPublicAPi() throws Exception {
    final WorkspaceCreateRequest workspaceCreateRequest = new WorkspaceCreateRequest("destinations-workspace");
    final CreateWorkspaceResponse workspaceResponse = airbyteApiWorkspacesClient.createWorkspace(workspaceCreateRequest);

    final UUID newWorkspaceId = UUID.fromString(workspaceResponse.workspaceResponse.workspaceId);

    testHarness.createDestination(
        "listDestinationsTest" + UUID.randomUUID(),
        newWorkspaceId,
        testHarness.getPostgresDestinationDefinitionId(),
        testHarness.getDestinationDbConfig());

    testHarness.createDestination(
        "listDestinationsTest" + UUID.randomUUID(),
        newWorkspaceId,
        testHarness.getPostgresDestinationDefinitionId(),
        testHarness.getDestinationDbConfig());

    final List<DestinationRead> expectedDestinations = configApiClient.getDestinationApi().listDestinationsForWorkspace(
        new WorkspaceIdRequestBody().workspaceId(newWorkspaceId)).getDestinations();
    final List<UUID> expectedDestinationIds = expectedDestinations.stream().map(DestinationRead::getDestinationId).sorted().toList();

    final ListDestinationsRequest listDestinationsRequest = new ListDestinationsRequest()
        .withWorkspaceIds(new String[] {newWorkspaceId.toString()})
        .withIncludeDeleted(false);
    final List<UUID> destinationResponseList =
        Arrays.stream(airbyteApiDestinationsClient.listDestinations(listDestinationsRequest).destinationsResponse.data)
            .map(destinationResponse -> UUID.fromString(destinationResponse.destinationId)).sorted().toList();
    // Since we retry the createSource api call, it is possible for more destinations than expected to
    // be
    // created. Assert a containsAll instead of equals.
    assertTrue(destinationResponseList.containsAll(expectedDestinationIds));

    final ListDestinationsRequest listAllDestinationsRequest = new ListDestinationsRequest()
        .withIncludeDeleted(false)
        .withLimit(1000);
    final List<UUID> destinationResponseListNullWorkspaceIds =
        Arrays.stream(airbyteApiDestinationsClient.listDestinations(listAllDestinationsRequest).destinationsResponse.data)
            .map(destinationResponse -> UUID.fromString(destinationResponse.destinationId)).sorted().toList();
    // Since we retry the createSource api call, it is possible for more destinations than expected to
    // be
    // created. Assert a containsAll instead of equals.
    assertTrue(destinationResponseListNullWorkspaceIds.containsAll(expectedDestinationIds));

    final ListDestinationsRequest listDestinationsRequestNoPage = new ListDestinationsRequest()
        .withWorkspaceIds(new String[] {newWorkspaceId.toString()})
        .withLimit(0)
        .withIncludeDeleted(false);
    final DestinationResponse[] destinationResponsesNoPage =
        airbyteApiDestinationsClient.listDestinations(listDestinationsRequestNoPage).destinationsResponse.data;
    assertNull(destinationResponsesNoPage);

    final ListDestinationsRequest listDestinationsRequestOffset = new ListDestinationsRequest()
        .withWorkspaceIds(new String[] {newWorkspaceId.toString()})
        .withOffset(1)
        .withIncludeDeleted(false);
    final List<DestinationResponse> destinationResponsesOffset =
        Arrays.asList(airbyteApiDestinationsClient.listDestinations(listDestinationsRequestOffset).destinationsResponse.data);
    assertEquals(1, destinationResponsesOffset.size());
  }

  @Disabled
  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE)
  @Timeout(value = 10,
           unit = TimeUnit.MINUTES)
  void testUpdateSourcePartial() throws Exception {
    final String dbName = "acc-test-db";
    final UUID postgresSourceDefinitionId = testHarness.getPostgresSourceDefinitionId();
    final JsonNode sourceDbConfig = testHarness.getSourceDbConfig();

    final SourceRead createResponse = testHarness.createSource(
        dbName,
        workspaceId,
        postgresSourceDefinitionId,
        sourceDbConfig);

    final PatchSourceRequest patchSourceRequest = new PatchSourceRequest(createResponse.getSourceId().toString())
        .withSourcePatchRequest(new SourcePatchRequest().withConfiguration(Jsons.emptyObject()));

    final PatchSourceResponse noOpUpdateResponse = airbyteApiSourcesClient.patchSource(patchSourceRequest);

    final SourceResponse expectedResponse = new SourceResponse(
        createResponse.getConnectionConfiguration(),
        createResponse.getName(),
        createResponse.getSourceId().toString(),
        SourcePostgresPostgres.POSTGRES.value,
        createResponse.getWorkspaceId().toString());
    assertEquals(expectedResponse.name, noOpUpdateResponse.sourceResponse.name);
    assertEquals(Jsons.jsonNode(expectedResponse.configuration), Jsons.jsonNode(noOpUpdateResponse.sourceResponse.configuration));

    final PatchSourceRequest patchSourceRequestName = new PatchSourceRequest(createResponse.getSourceId().toString())
        .withSourcePatchRequest(
            new SourcePatchRequest()
                .withName(TEST_ACTOR_NAME)
                .withConfiguration(Jsons.emptyObject()));
    final SourceResponse nameUpdateResponse = airbyteApiSourcesClient.patchSource(patchSourceRequestName).sourceResponse;
    final SourceResponse expectedNameUpdateResponse = new SourceResponse(
        createResponse.getConnectionConfiguration(),
        TEST_ACTOR_NAME,
        createResponse.getSourceId().toString(),
        POSTGRES.value,
        createResponse.getWorkspaceId().toString());
    assertEquals(expectedNameUpdateResponse.name, nameUpdateResponse.name);
    assertEquals(Jsons.jsonNode(expectedNameUpdateResponse.configuration), Jsons.jsonNode(nameUpdateResponse.configuration));

    final int fakePort = 12345;

    final JsonNode newConfig = Jsons.jsonNode(Map.of(JdbcUtils.PORT_KEY, fakePort));
    final JsonNode expectedConfig = createResponse.getConnectionConfiguration().deepCopy();
    ((ObjectNode) expectedConfig).put(JdbcUtils.PORT_KEY, fakePort);
    final PatchSourceRequest patchSourceRequestConfig = new PatchSourceRequest(createResponse.getSourceId().toString())
        .withSourcePatchRequest(
            new SourcePatchRequest()
                .withConfiguration(newConfig));
    final SourceResponse configUpdateResponse = airbyteApiSourcesClient.patchSource(patchSourceRequestConfig).sourceResponse;
    final SourceResponse expectedConfigUpdateResponse = new SourceResponse(
        expectedConfig,
        TEST_ACTOR_NAME,
        createResponse.getSourceId().toString(),
        POSTGRES.value,
        createResponse.getWorkspaceId().toString());

    assertEquals(expectedConfigUpdateResponse.name, configUpdateResponse.name);
    assertEquals(Jsons.jsonNode(expectedConfigUpdateResponse.configuration), Jsons.jsonNode(configUpdateResponse.configuration));
  }

  @Disabled
  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE)
  @Timeout(value = 10,
           unit = TimeUnit.MINUTES)
  void testUpdateDestinationPartial() throws Exception {
    final String dbName = "acc-test-db";
    final UUID postgresDestinationDefinitionId = testHarness.getPostgresDestinationDefinitionId();
    final JsonNode destinationDbConfig = testHarness.getDestinationDbConfig();

    final DestinationRead createResponse = testHarness.createDestination(
        dbName,
        workspaceId,
        postgresDestinationDefinitionId,
        destinationDbConfig);

    final PatchDestinationRequest patchDestinationRequestNoop = new PatchDestinationRequest(createResponse.getDestinationId().toString())
        .withDestinationPatchRequest(new DestinationPatchRequest().withConfiguration(Jsons.emptyObject()));

    final DestinationResponse noOpUpdateResponse = airbyteApiDestinationsClient.patchDestination(patchDestinationRequestNoop).destinationResponse;
    final DestinationResponse expectedResponse = new DestinationResponse(
        createResponse.getConnectionConfiguration(),
        createResponse.getDestinationId().toString(),
        POSTGRES.value,
        createResponse.getName(),
        createResponse.getWorkspaceId().toString());
    assertEquals(expectedResponse.name, noOpUpdateResponse.name);
    assertEquals(Jsons.jsonNode(expectedResponse.configuration), Jsons.jsonNode(noOpUpdateResponse.configuration));

    final PatchDestinationRequest patchDestinationRequestName = new PatchDestinationRequest(createResponse.getDestinationId().toString())
        .withDestinationPatchRequest(
            new DestinationPatchRequest()
                .withName(TEST_ACTOR_NAME)
                .withConfiguration(Jsons.emptyObject()));
    final DestinationResponse nameUpdateResponse = airbyteApiDestinationsClient.patchDestination(patchDestinationRequestName).destinationResponse;
    final DestinationResponse expectedNameUpdateResponse = new DestinationResponse(
        createResponse.getConnectionConfiguration(),
        createResponse.getDestinationId().toString(),
        POSTGRES.value,
        TEST_ACTOR_NAME,
        createResponse.getWorkspaceId().toString());
    assertEquals(expectedNameUpdateResponse.name, nameUpdateResponse.name);
    assertEquals(Jsons.jsonNode(expectedNameUpdateResponse.configuration), Jsons.jsonNode(nameUpdateResponse.configuration));

    final int fakePort = 12345;

    final JsonNode newConfig = Jsons.jsonNode(Map.of(JdbcUtils.PORT_KEY, fakePort));
    final JsonNode expectedConfig = createResponse.getConnectionConfiguration().deepCopy();
    ((ObjectNode) expectedConfig).put(JdbcUtils.PORT_KEY, fakePort);
    final PatchDestinationRequest patchDestinationRequestConfig = new PatchDestinationRequest(createResponse.getDestinationId().toString())
        .withDestinationPatchRequest(
            new DestinationPatchRequest()
                .withConfiguration(newConfig));
    final DestinationResponse configUpdateResponse = airbyteApiDestinationsClient.patchDestination(patchDestinationRequestConfig).destinationResponse;
    final DestinationResponse expectedConfigUpdateResponse = new DestinationResponse(
        expectedConfig,
        createResponse.getDestinationId().toString(),
        POSTGRES.value,
        TEST_ACTOR_NAME,
        createResponse.getWorkspaceId().toString());
    assertEquals(expectedConfigUpdateResponse.name, configUpdateResponse.name);
    assertEquals(Jsons.jsonNode(expectedConfigUpdateResponse.configuration), Jsons.jsonNode(configUpdateResponse.configuration));
  }

  @Disabled
  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE)
  @Timeout(value = 5,
           unit = TimeUnit.MINUTES)
  void testUpdateConnection() throws Exception {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final UUID normalizationOpId = testHarness.createNormalizationOperation().getOperationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();

    final ConnectionRead createdConnection =
        AirbyteApiClient.retryWithJitter(() -> testHarness
            .createConnection(new TestConnectionCreate.Builder(
                sourceId,
                destinationId,
                catalog,
                discoverResult.getCatalogId())
                    .setNormalizationOperationId(normalizationOpId)
                    .build()),
            "connection create",
            JITTER_MAX_INTERVAL_SECS,
            FINAL_INTERVAL_SECS,
            MAX_TRIES);

    final PatchConnectionRequest patchConnectionRequestNoOp = new PatchConnectionRequest(
        new ConnectionPatchRequest().withConfigurations(null), createdConnection.getConnectionId().toString());
    final ConnectionResponse noopUpdateResponse = airbyteApiConnectionsClient.patchConnection(patchConnectionRequestNoOp).connectionResponse;

    final ConnectionRead noopUpdatedConnection =
        configApiConnectionsClient.getConnection(new ConnectionIdRequestBody().connectionId(UUID.fromString(noopUpdateResponse.connectionId)));

    assertEquals(createdConnection, noopUpdatedConnection);

    final String newName = "new-name";
    final PatchConnectionRequest patchConnectionRequestNameStatus = new PatchConnectionRequest(
        new ConnectionPatchRequest().withName(newName).withStatus(ConnectionStatusEnum.INACTIVE), createdConnection.getConnectionId().toString());
    final ConnectionResponse updateResponse = airbyteApiConnectionsClient.patchConnection(patchConnectionRequestNameStatus).connectionResponse;

    final ConnectionRead updatedConnection =
        configApiConnectionsClient.getConnection(new ConnectionIdRequestBody().connectionId(UUID.fromString(updateResponse.connectionId)));

    assertEquals(updatedConnection.getName(), newName);
    assertEquals(updatedConnection.getStatus(), ConnectionStatus.INACTIVE);
  }

}
