/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ActorDefinitionRequestBody;
import io.airbyte.api.client.model.generated.ActorType;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AirbyteStream;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.client.model.generated.AttemptInfoRead;
import io.airbyte.api.client.model.generated.CheckConnectionRead;
import io.airbyte.api.client.model.generated.CheckConnectionRead.Status;
import io.airbyte.api.client.model.generated.ConnectionCreate;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionReadList;
import io.airbyte.api.client.model.generated.ConnectionScheduleData;
import io.airbyte.api.client.model.generated.ConnectionScheduleType;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.api.client.model.generated.ConnectionUpdate;
import io.airbyte.api.client.model.generated.CustomDestinationDefinitionCreate;
import io.airbyte.api.client.model.generated.CustomSourceDefinitionCreate;
import io.airbyte.api.client.model.generated.DataplaneGroupListRequestBody;
import io.airbyte.api.client.model.generated.DestinationCreate;
import io.airbyte.api.client.model.generated.DestinationDefinitionCreate;
import io.airbyte.api.client.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.client.model.generated.DestinationDefinitionRead;
import io.airbyte.api.client.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.client.model.generated.DestinationDefinitionUpdate;
import io.airbyte.api.client.model.generated.DestinationIdRequestBody;
import io.airbyte.api.client.model.generated.DestinationRead;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.DestinationUpdate;
import io.airbyte.api.client.model.generated.GetAttemptStatsRequestBody;
import io.airbyte.api.client.model.generated.JobConfigType;
import io.airbyte.api.client.model.generated.JobDebugInfoRead;
import io.airbyte.api.client.model.generated.JobIdRequestBody;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobListForWorkspacesRequestBody;
import io.airbyte.api.client.model.generated.JobListRequestBody;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.JobStatus;
import io.airbyte.api.client.model.generated.JobWithAttemptsRead;
import io.airbyte.api.client.model.generated.ListResourcesForWorkspacesRequestBody;
import io.airbyte.api.client.model.generated.LogFormatType;
import io.airbyte.api.client.model.generated.NamespaceDefinitionType;
import io.airbyte.api.client.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.client.model.generated.OperationCreate;
import io.airbyte.api.client.model.generated.OperationIdRequestBody;
import io.airbyte.api.client.model.generated.OperationRead;
import io.airbyte.api.client.model.generated.OperatorConfiguration;
import io.airbyte.api.client.model.generated.OperatorType;
import io.airbyte.api.client.model.generated.OperatorWebhook;
import io.airbyte.api.client.model.generated.OperatorWebhookDbtCloud;
import io.airbyte.api.client.model.generated.Pagination;
import io.airbyte.api.client.model.generated.SchemaChangeBackfillPreference;
import io.airbyte.api.client.model.generated.SourceCreate;
import io.airbyte.api.client.model.generated.SourceDefinitionCreate;
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.client.model.generated.SourceDefinitionRead;
import io.airbyte.api.client.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.api.client.model.generated.SourceDefinitionUpdate;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRequestBody;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.api.client.model.generated.SourceRead;
import io.airbyte.api.client.model.generated.SourceReadList;
import io.airbyte.api.client.model.generated.SourceUpdate;
import io.airbyte.api.client.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.client.model.generated.StreamStatusReadList;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.api.client.model.generated.WebBackendConnectionRead;
import io.airbyte.api.client.model.generated.WebBackendConnectionRequestBody;
import io.airbyte.api.client.model.generated.WebBackendConnectionUpdate;
import io.airbyte.api.client.model.generated.WebBackendOperationCreateOrUpdate;
import io.airbyte.api.client.model.generated.WorkspaceCreateWithId;
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.string.Strings;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.temporal.TemporalWorkflowUtils;
import io.airbyte.commons.temporal.config.TemporalSdkTimeouts;
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow;
import io.airbyte.commons.temporal.scheduling.state.WorkflowState;
import io.airbyte.db.Database;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Flag;
import io.airbyte.featureflag.tests.TestFlagsSetter;
import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import junit.framework.AssertionFailedError;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.Assertions;
import org.openapitools.client.infrastructure.ClientException;
import org.openapitools.client.infrastructure.ServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * This class contains containers used for acceptance tests. Some of those containers/states are
 * only used when the test are run without GKE. Specific environmental variables govern what types
 * of containers are run.
 * <p>
 * This class is put in a separate module to be easily pulled in as a dependency for Airbyte Cloud
 * Acceptance Tests.
 * <p>
 * Containers and states include:
 * <li>source postgres SQL</li>
 * <li>destination postgres SQL</li>
 * <li>kubernetes client</li>
 * <li>lists of UUIDS representing IDs of sources, destinations, connections, and operations</li>
 */
@SuppressWarnings({"PMD.AvoidDuplicateLiterals", "PMD.LiteralsFirstInComparisons"})
public class AcceptanceTestHarness {

  private static final Logger LOGGER = LoggerFactory.getLogger(AcceptanceTestHarness.class);

  private static final UUID DEFAULT_ORGANIZATION_ID = io.airbyte.commons.ConstantsKt.DEFAULT_ORGANIZATION_ID;
  private static final DockerImageName DESTINATION_POSTGRES_IMAGE_NAME = DockerImageName.parse("postgres:15-alpine");

  private static final DockerImageName SOURCE_POSTGRES_IMAGE_NAME = DockerImageName.parse("debezium/postgres:15-alpine")
      .asCompatibleSubstituteFor("postgres");

  private static final String TEMPORAL_HOST = java.util.Optional.ofNullable(System.getenv("TEMPORAL_HOST")).orElse("temporal.airbyte.dev:80");

  private static final String SOURCE_E2E_TEST_CONNECTOR_VERSION = "0.1.2";
  private static final String DESTINATION_E2E_TEST_CONNECTOR_VERSION = "0.1.1";

  public static final String POSTGRES_DESTINATION_CONNECTOR_VERSION = "0.6.3";
  public static final String POSTGRES_SOURCE_LEGACY_CONNECTOR_VERSION = "0.4.26";

  public static final String OUTPUT_STREAM_PREFIX = "output_table_";
  private static final String TABLE_NAME = "id_and_name";
  public static final String STREAM_NAME = TABLE_NAME;
  public static final String COLUMN_ID = "id";
  public static final String COLUMN_NAME = "name";
  private static final String SOURCE_USERNAME = "sourceusername";
  public static final String SOURCE_PASSWORD = "hunter2";
  public static final String PUBLIC_SCHEMA_NAME = "public";
  public static final String PUBLIC = "public";

  private static final String DEFAULT_POSTGRES_INIT_SQL_FILE = "postgres_init.sql";

  public static final int JITTER_MAX_INTERVAL_SECS = 10;
  public static final int FINAL_INTERVAL_SECS = 60;
  public static final int MAX_TRIES = 5;
  public static final int MAX_ALLOWED_SECOND_PER_RUN = 120;

  private static final String CLOUD_SQL_DATABASE_PREFIX = "acceptance_test_";

  // NOTE: we include `INCOMPLETE` here because the job may still retry; see
  // https://docs.airbyte.com/understanding-airbyte/jobs/.
  public static final Set<JobStatus> IN_PROGRESS_JOB_STATUSES = Set.of(JobStatus.PENDING, JobStatus.INCOMPLETE, JobStatus.RUNNING);

  private static final String KUBE_PROCESS_RUNNER_HOST = java.util.Optional.ofNullable(System.getenv("KUBE_PROCESS_RUNNER_HOST")).orElse("");
  private static final String EXPECTED_JSON_SCHEMA = """
                                                     {
                                                       "type": "object",
                                                       "properties": {
                                                         "%s": {
                                                           "type": "string"
                                                         },
                                                         "%s": {
                                                           "airbyte_type": "integer",
                                                           "type": "number"
                                                         }
                                                       }
                                                     }
                                                     """.formatted(COLUMN_NAME, COLUMN_ID);

  private static boolean isKube;
  private static boolean isMinikube;
  private static boolean isGke;
  private static boolean isCI;
  private static boolean isMac;
  private static boolean ensureCleanSlate;
  private CloudSqlDatabaseProvisioner cloudSqlDatabaseProvisioner;

  /**
   * When the acceptance tests are run against a local instance of docker-compose or KUBE then these
   * test containers are used. When we run these tests in GKE, we spawn a source and destination
   * postgres database ane use them for testing.
   */
  private PostgreSQLContainer sourcePsql;
  private PostgreSQLContainer destinationPsql;
  private String sourceDatabaseName;
  private String destinationDatabaseName;

  private final AirbyteApiClient apiClient;
  private final TestFlagsSetter testFlagsSetter;
  private final UUID defaultWorkspaceId;
  private final UUID dataplaneGroupId;
  private final String postgresSqlInitFile;

  private final List<UUID> sourceIds = Lists.newArrayList();
  private final List<UUID> connectionIds = Lists.newArrayList();
  private final List<UUID> destinationIds = Lists.newArrayList();
  private final List<UUID> operationIds = Lists.newArrayList();
  private final List<UUID> sourceDefinitionIds = Lists.newArrayList();
  private DataSource sourceDataSource;
  private DataSource destinationDataSource;
  private final AirbyteCatalog expectedAirbyteCatalog;

  private String gcpProjectId;
  private String cloudSqlInstanceId;
  private String cloudSqlInstanceUsername;
  private String cloudSqlInstancePassword;
  private String cloudSqlInstancePublicIp;
  private final RetryPolicy<Object> retryPolicy;

  public void removeConnection(final UUID connection) {
    connectionIds.remove(connection);
  }

  public AcceptanceTestHarness(final AirbyteApiClient apiClient,
                               final UUID defaultWorkspaceId,
                               final String postgresSqlInitFile)
      throws GeneralSecurityException, URISyntaxException, IOException, InterruptedException {
    this(apiClient, null, defaultWorkspaceId, postgresSqlInitFile);
  }

  public AcceptanceTestHarness(final AirbyteApiClient apiClient,
                               final TestFlagsSetter testFlagsSetter,
                               final UUID defaultWorkspaceId,
                               final String postgresSqlInitFile)
      throws URISyntaxException, IOException, InterruptedException, GeneralSecurityException {
    // reads env vars to assign static variables
    assignEnvVars();
    this.apiClient = apiClient;
    this.testFlagsSetter = testFlagsSetter;
    this.defaultWorkspaceId = defaultWorkspaceId;
    this.postgresSqlInitFile = postgresSqlInitFile;

    if (isGke && !isKube) {
      throw new RuntimeException("KUBE Flag should also be enabled if GKE flag is enabled");
    }
    if (!isGke) {
      sourcePsql = new PostgreSQLContainer(SOURCE_POSTGRES_IMAGE_NAME);
      sourcePsql.withUsername(SOURCE_USERNAME)
          .withPassword(SOURCE_PASSWORD);
      sourcePsql.start();

      destinationPsql = new PostgreSQLContainer(DESTINATION_POSTGRES_IMAGE_NAME);
      destinationPsql.start();
    } else {
      this.cloudSqlDatabaseProvisioner = new CloudSqlDatabaseProvisioner();
      sourceDatabaseName = cloudSqlDatabaseProvisioner.createDatabase(
          gcpProjectId,
          cloudSqlInstanceId,
          generateRandomCloudSqlDatabaseName());
      destinationDatabaseName = cloudSqlDatabaseProvisioner.createDatabase(
          gcpProjectId,
          cloudSqlInstanceId,
          generateRandomCloudSqlDatabaseName());
    }

    final JsonNode expectedSchema = Jsons.deserialize(EXPECTED_JSON_SCHEMA);
    final AirbyteStream expectedStream = new AirbyteStream(
        STREAM_NAME,
        expectedSchema,
        List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
        false,
        List.of(),
        List.of(),
        PUBLIC,
        true,
        null);
    final AirbyteStreamConfiguration expectedStreamConfig = new AirbyteStreamConfiguration(
        SyncMode.FULL_REFRESH,
        DestinationSyncMode.OVERWRITE,
        List.of(),
        null,
        List.of(),
        STREAM_NAME.replace(".", "_"),
        true,
        true,
        null,
        null,
        null,
        List.of(),
        List.of(),
        List.of(),
        null,
        null,
        null);
    expectedAirbyteCatalog = new AirbyteCatalog(
        List.of(
            new AirbyteStreamAndConfiguration(expectedStream, expectedStreamConfig)));

    retryPolicy = RetryPolicy.builder()
        .handle(List.of(Exception.class))
        .withBackoff(Duration.ofSeconds(JITTER_MAX_INTERVAL_SECS), Duration.ofSeconds(FINAL_INTERVAL_SECS))
        .withMaxRetries(MAX_TRIES)
        .build();

    dataplaneGroupId = apiClient.getDataplaneGroupApi().listDataplaneGroups(new DataplaneGroupListRequestBody(DEFAULT_ORGANIZATION_ID))
        .getDataplaneGroups().getFirst().getDataplaneGroupId();

    LOGGER.info("Using external deployment of airbyte.");
  }

  public AcceptanceTestHarness(final AirbyteApiClient apiClient, final UUID defaultWorkspaceId)
      throws URISyntaxException, IOException, InterruptedException, GeneralSecurityException {
    this(apiClient, defaultWorkspaceId, DEFAULT_POSTGRES_INIT_SQL_FILE);
  }

  public AcceptanceTestHarness(final AirbyteApiClient apiClient, final UUID defaultWorkspaceId, final TestFlagsSetter testFlagsSetter)
      throws GeneralSecurityException, URISyntaxException, IOException, InterruptedException {
    this(apiClient, testFlagsSetter, defaultWorkspaceId, DEFAULT_POSTGRES_INIT_SQL_FILE);
  }

  public AirbyteApiClient getApiClient() {
    return apiClient;
  }

  public TestFlagsSetter getTestFlagsSetter() {
    return testFlagsSetter;
  }

  public void stopDbAndContainers() {
    if (isGke) {
      try {
        DataSourceFactory.close(sourceDataSource);
        DataSourceFactory.close(destinationDataSource);
      } catch (final Exception e) {
        LOGGER.warn("Failed to close data sources", e);
      }
    } else {
      sourcePsql.stop();
      destinationPsql.stop();
    }
  }

  public void setup() throws SQLException, URISyntaxException, IOException {
    if (isGke) {
      // Prepare the database data sources.
      LOGGER.info("postgresPassword: {}", cloudSqlInstancePassword);
      sourceDataSource = GKEPostgresConfig.getDataSource(
          cloudSqlInstanceUsername,
          cloudSqlInstancePassword,
          cloudSqlInstancePublicIp,
          sourceDatabaseName);
      destinationDataSource = GKEPostgresConfig.getDataSource(
          cloudSqlInstanceUsername,
          cloudSqlInstancePassword,
          cloudSqlInstancePublicIp,
          destinationDatabaseName);
      // seed database.
      GKEPostgresConfig.runSqlScript(Path.of(MoreResources.readResourceAsFile(postgresSqlInitFile).toURI()), getSourceDatabase());
    } else {
      PostgreSQLContainerHelper.runSqlScript(MountableFile.forClasspathResource(postgresSqlInitFile), sourcePsql);

      sourceDataSource = Databases.createDataSource(sourcePsql);
      destinationDataSource = Databases.createDataSource(destinationPsql);

      // Pinning Postgres destination version. This doesn't work on GKE since the
      // airbyte-cron will revert this change. On GKE we are pinning the version by
      // adding an entry to the scoped_configuration table.
      final DestinationDefinitionRead postgresDestDef = getPostgresDestinationDefinition();
      if (!postgresDestDef.getDockerImageTag().equals(POSTGRES_DESTINATION_CONNECTOR_VERSION)) {
        LOGGER.info("Setting postgres destination connector to version {}...", POSTGRES_DESTINATION_CONNECTOR_VERSION);
        try {
          updateDestinationDefinitionVersion(postgresDestDef.getDestinationDefinitionId(), POSTGRES_DESTINATION_CONNECTOR_VERSION);
        } catch (final ClientException | ServerException e) {
          LOGGER.error("Error while updating destination definition version", e);
        }
      }
    }
  }

  public void cleanup() {
    try {
      clearSourceDbData();
      clearDestinationDbData();
      for (final UUID operationId : operationIds) {
        deleteOperation(operationId);
      }

      for (final UUID connectionId : connectionIds) {
        disableConnection(connectionId);
      }

      for (final UUID sourceId : sourceIds) {
        deleteSource(sourceId);
      }

      // TODO(mfsiega-airbyte): clean up source definitions that get created.

      for (final UUID destinationId : destinationIds) {
        deleteDestination(destinationId);
      }
      if (isGke) {
        DataSourceFactory.close(sourceDataSource);
        DataSourceFactory.close(destinationDataSource);

        cloudSqlDatabaseProvisioner.deleteDatabase(
            gcpProjectId,
            cloudSqlInstanceId,
            sourceDatabaseName);
        cloudSqlDatabaseProvisioner.deleteDatabase(
            gcpProjectId,
            cloudSqlInstanceId,
            destinationDatabaseName);
      } else {
        destinationPsql.stop();
        sourcePsql.stop();
      }
      // TODO(mfsiega-airbyte): clean up created source definitions.
    } catch (final Exception e) {
      LOGGER.error("Error tearing down test fixtures", e);
    }
  }

  /**
   * This method is intended to be called at the beginning of a new test run - it identifies and
   * disables any pre-existing scheduled connections that could potentially interfere with a new test
   * run.
   */
  public void ensureCleanSlate() {
    if (!ensureCleanSlate) {
      LOGGER.info("proceeding without cleaning up pre-existing connections.");
      return;
    }
    LOGGER.info("ENSURE_CLEAN_SLATE was true, disabling all scheduled connections using postgres source or postgres destination...");
    try {
      final UUID sourceDefinitionId = getPostgresSourceDefinitionId();
      final UUID destinationDefinitionId = getPostgresDestinationDefinitionId();

      final List<ConnectionRead> sourceDefinitionConnections = this.apiClient.getConnectionApi()
          .listConnectionsByActorDefinition(
              new ActorDefinitionRequestBody(sourceDefinitionId, ActorType.SOURCE))
          .getConnections();

      final List<ConnectionRead> destinationDefinitionConnections = this.apiClient.getConnectionApi()
          .listConnectionsByActorDefinition(
              new ActorDefinitionRequestBody(destinationDefinitionId, ActorType.DESTINATION))
          .getConnections();

      final Set<ConnectionRead> allConnections = Sets.newHashSet();
      allConnections.addAll(sourceDefinitionConnections);
      allConnections.addAll(destinationDefinitionConnections);

      final List<ConnectionRead> allConnectionsToDisable = allConnections.stream()
          // filter out any connections that aren't active
          .filter(connection -> connection.getStatus().equals(ConnectionStatus.ACTIVE))
          // filter out any manual connections, since we only want to disable scheduled syncs
          .filter(connection -> !connection.getScheduleType().equals(ConnectionScheduleType.MANUAL))
          .toList();

      LOGGER.info("Found {} existing connection(s) to clean up", allConnectionsToDisable.size());
      if (!allConnectionsToDisable.isEmpty()) {
        for (final ConnectionRead connection : allConnectionsToDisable) {
          disableConnection(connection.getConnectionId());
          LOGGER.info("disabled connection with ID {}", connection.getConnectionId());
        }
      }
      LOGGER.info("ensureCleanSlate completed!");
    } catch (final Exception e) {
      LOGGER.warn("An exception occurred while ensuring a clean slate. Proceeding, but a clean slate is not guaranteed for this run.", e);
    }
  }

  @SuppressWarnings("PMD.LiteralsFirstInComparisons")
  private void assignEnvVars() {
    isKube = System.getenv().containsKey("KUBE");
    isMinikube = System.getenv().containsKey("IS_MINIKUBE");
    isGke = System.getenv().containsKey("IS_GKE");
    isCI = System.getenv().containsKey("CI");
    isMac = System.getProperty("os.name").startsWith("Mac");
    ensureCleanSlate = System.getenv("ENSURE_CLEAN_SLATE") != null
        && System.getenv("ENSURE_CLEAN_SLATE").equalsIgnoreCase("true");
    gcpProjectId = System.getenv("GCP_PROJECT_ID");
    cloudSqlInstanceId = System.getenv("CLOUD_SQL_INSTANCE_ID");
    cloudSqlInstanceUsername = System.getenv("CLOUD_SQL_INSTANCE_USERNAME");
    cloudSqlInstancePassword = System.getenv("CLOUD_SQL_INSTANCE_PASSWORD");
    cloudSqlInstancePublicIp = System.getenv("CLOUD_SQL_INSTANCE_PUBLIC_IP");
  }

  private WorkflowClient getWorkflowClient() {
    final TemporalUtils temporalUtils = new TemporalUtils(null, null, null,
        null, null, null,
        null, Optional.empty());
    final WorkflowServiceStubs temporalService = temporalUtils.createTemporalService(
        TemporalWorkflowUtils.getAirbyteTemporalOptions(TEMPORAL_HOST, new TemporalSdkTimeouts()),
        TemporalUtils.DEFAULT_NAMESPACE);
    return WorkflowClient.newInstance(temporalService);
  }

  public WorkflowState getWorkflowState(final UUID connectionId) {
    final WorkflowClient workflowCLient = getWorkflowClient();

    // check if temporal workflow is reachable
    final ConnectionManagerWorkflow connectionManagerWorkflow =
        workflowCLient.newWorkflowStub(ConnectionManagerWorkflow.class, "connection_manager_" + connectionId);
    return connectionManagerWorkflow.getState();
  }

  public void terminateTemporalWorkflow(final UUID connectionId) {
    final WorkflowClient workflowCLient = getWorkflowClient();

    // check if temporal workflow is reachable
    getWorkflowState(connectionId);

    // Terminate workflow
    LOGGER.info("Terminating temporal workflow...");
    workflowCLient.newUntypedWorkflowStub("connection_manager_" + connectionId).terminate("");

    // remove connection to avoid exception during tear down
    connectionIds.remove(connectionId);
  }

  public AirbyteCatalog discoverSourceSchema(final UUID sourceId) throws IOException {
    return discoverSourceSchemaWithId(sourceId).getCatalog();
  }

  public SourceDiscoverSchemaRead discoverSourceSchemaWithId(final UUID sourceId) throws IOException {
    return Failsafe.with(retryPolicy).get(() -> {
      final var result =
          apiClient.getSourceApi().discoverSchemaForSource(new SourceDiscoverSchemaRequestBody(sourceId, null, true, null));
      if (result.getCatalog() == null) {
        throw new RuntimeException("no catalog returned, retrying...");
      }
      return result;
    });
  }

  // Run check Connection workflow.
  public void checkConnection(final UUID sourceId) throws IOException {
    apiClient.getSourceApi().checkConnectionToSource(new SourceIdRequestBody(sourceId));
  }

  public AirbyteCatalog discoverSourceSchemaWithoutCache(final UUID sourceId) throws IOException {
    return apiClient.getSourceApi().discoverSchemaForSource(
        new SourceDiscoverSchemaRequestBody(sourceId, null, true, null)).getCatalog();
  }

  public DestinationDefinitionSpecificationRead getDestinationDefinitionSpec(final UUID destinationDefinitionId, final UUID workspaceId)
      throws IOException {
    return apiClient.getDestinationDefinitionSpecificationApi()
        .getDestinationDefinitionSpecification(
            new DestinationDefinitionIdWithWorkspaceId(destinationDefinitionId, workspaceId));
  }

  public SourceDefinitionSpecificationRead getSourceDefinitionSpec(final UUID sourceDefinitionId, final UUID workspaceId) throws IOException {
    return apiClient.getSourceDefinitionSpecificationApi()
        .getSourceDefinitionSpecification(
            new SourceDefinitionIdWithWorkspaceId(sourceDefinitionId, workspaceId));
  }

  public Database getSourceDatabase() {
    return getDatabase(sourceDataSource);
  }

  public Database getDestinationDatabase() {
    return getDatabase(destinationDataSource);
  }

  public Database getDatabase(final DataSource dataSource) {
    return new Database(Databases.createDslContext(dataSource, SQLDialect.POSTGRES));
  }

  public UUID getDataplaneGroupId() {
    return dataplaneGroupId;
  }

  /**
   * Assert that the normalized destination matches the input records, only expecting a single id
   * column.
   *
   * @param expectedRecords the records that we expect
   * @throws Exception while retrieving sources
   */
  public void assertNormalizedDestinationContainsIdColumn(final String outputSchema, final List<JsonNode> expectedRecords) throws Exception {
    final Database destination = getDestinationDatabase();
    final String finalDestinationTable = String.format("%s.%s%s", outputSchema, OUTPUT_STREAM_PREFIX, STREAM_NAME.replace(".", "_"));
    final List<JsonNode> destinationRecords = retrieveRecordsFromDatabase(destination, finalDestinationTable);

    assertEquals(expectedRecords.size(), destinationRecords.size(),
        String.format("source contains: %s record. destination contains: %s", expectedRecords.size(), destinationRecords.size()));

    // Assert that each expected record id is present in the actual records.
    for (final JsonNode expectedRecord : expectedRecords) {
      assertTrue(
          destinationRecords.stream()
              .anyMatch(r -> r.get(COLUMN_ID).asInt() == expectedRecord.get(COLUMN_ID).asInt()),
          String.format("destination does not contain record:\n %s \n destination contains:\n %s\n", expectedRecord, destinationRecords));
    }

    for (final JsonNode actualRecord : destinationRecords) {
      final var fieldNamesIterator = actualRecord.fieldNames();
      while (fieldNamesIterator.hasNext()) {
        final String fieldName = fieldNamesIterator.next();
        // NOTE: we filtered this column out, so we check that it isn't present.
        assertNotEquals(fieldName, COLUMN_NAME);
      }
    }
  }

  public void runSqlScriptInSource(final String resourceName) throws URISyntaxException, SQLException, IOException {
    LOGGER.debug("Running sql script in source: {}", resourceName);
    if (isGke) {
      GKEPostgresConfig.runSqlScript(Path.of(MoreResources.readResourceAsFile(resourceName).toURI()), getSourceDatabase());
    } else {
      PostgreSQLContainerHelper.runSqlScript(MountableFile.forClasspathResource(resourceName), sourcePsql);
    }
  }

  public ConnectionRead createConnection(final TestConnectionCreate create)
      throws Exception {

    /*
     * We control the name inside this method to avoid collisions of sync name and namespace. Especially
     * if namespaces collide. This can cause tests to flake as they will be writing to the same tables
     * in the destination.
     */
    final String slug = RandomStringUtils.randomAlphabetic(5).toLowerCase();
    final String name = "accp-test-connection-" + slug + (create.getNameSuffix() != null ? "-" + create.getNameSuffix() : "");
    final String namespace = "accp_test_" + slug;

    return createConnectionFromRequest(new ConnectionCreate(
        create.getSrcId(),
        create.getDstId(),
        ConnectionStatus.ACTIVE,
        name,
        NamespaceDefinitionType.CUSTOMFORMAT,
        namespace,
        OUTPUT_STREAM_PREFIX,
        create.getOperationIds(),
        create.getConfiguredCatalog(),
        null,
        create.getScheduleType(),
        create.getScheduleData(),
        null,
        create.getCatalogId(),
        null,
        create.getDataplaneGroupId(),
        null,
        null,
        null,
        null,
        null));
  }

  public ConnectionRead createConnectionSourceNamespace(final TestConnectionCreate create)
      throws Exception {

    /*
     * We control the name inside this method to avoid collisions of sync name and namespace. Especially
     * if namespaces collide. This can cause tests to flake as they will be writing to the same tables
     * in the destination.
     */
    final String slug = RandomStringUtils.randomAlphabetic(5).toLowerCase();
    final String name = "accp-test-connection-" + slug + (create.getNameSuffix() != null ? "-" + create.getNameSuffix() : "");
    final String namespace = "accp_test_" + slug;

    return createConnectionFromRequest(
        new ConnectionCreate(
            create.getSrcId(),
            create.getDstId(),
            ConnectionStatus.ACTIVE,
            name,
            NamespaceDefinitionType.CUSTOMFORMAT,
            namespace + "_${SOURCE_NAMESPACE}",
            OUTPUT_STREAM_PREFIX,
            create.getOperationIds(),
            create.getConfiguredCatalog(),
            null,
            create.getScheduleType(),
            create.getScheduleData(),
            null,
            create.getCatalogId(),
            null,
            create.getDataplaneGroupId(),
            null,
            null,
            null,
            null,
            null));
  }

  private ConnectionRead createConnectionFromRequest(final ConnectionCreate request) throws IOException {
    final ConnectionRead connection = apiClient.getConnectionApi().createConnection(request);
    connectionIds.add(connection.getConnectionId());
    return connection;
  }

  public ConnectionRead getConnection(final UUID connectionId) throws IOException {
    return apiClient.getConnectionApi().getConnection(new ConnectionIdRequestBody(connectionId));
  }

  public void updateConnectionSchedule(
                                       final UUID connectionId,
                                       final ConnectionScheduleType newScheduleType,
                                       final ConnectionScheduleData newScheduleData)
      throws Exception {
    updateConnection(
        new ConnectionUpdate(
            connectionId,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            newScheduleType,
            newScheduleData,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null));
  }

  public void updateConnectionCatalog(final UUID connectionId, final AirbyteCatalog catalog) throws IOException, InterruptedException {
    updateConnection(
        new ConnectionUpdate(
            connectionId,
            null,
            null,
            null,
            null,
            null,
            catalog,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null));
  }

  public ConnectionRead updateConnectionSourceCatalogId(final UUID connectionId, final UUID sourceCatalogId)
      throws IOException, InterruptedException {
    return updateConnection(
        new ConnectionUpdate(
            connectionId,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            sourceCatalogId,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null));
  }

  private ConnectionRead updateConnection(final ConnectionUpdate request) throws IOException, InterruptedException {
    final var result = apiClient.getConnectionApi().updateConnection(request);
    // Attempting to sync immediately after updating the connection can run into a race condition in the
    // connection manager workflow hangs. This should be fixed in the backend, but for now we try to
    // tolerate it.
    Thread.sleep(1000 * 5);
    return result;
  }

  public JobInfoRead syncConnection(final UUID connectionId) throws IOException {
    return apiClient.getConnectionApi().syncConnection(new ConnectionIdRequestBody(connectionId));
  }

  public JobInfoRead cancelSync(final long jobId) throws IOException {
    return apiClient.getJobsApi().cancelJob(new JobIdRequestBody(jobId));
  }

  public JobInfoRead resetConnection(final UUID connectionId) throws IOException {
    return apiClient.getConnectionApi().resetConnection(new ConnectionIdRequestBody(connectionId));
  }

  public void deleteConnection(final UUID connectionId) throws IOException {
    apiClient.getConnectionApi().deleteConnection(new ConnectionIdRequestBody(connectionId));
  }

  public DestinationRead createPostgresDestination() throws IOException {
    return createPostgresDestination(defaultWorkspaceId);
  }

  public DestinationRead createPostgresDestination(final UUID workspaceId) throws IOException {
    return createDestination(
        "AccTestDestination-" + UUID.randomUUID(),
        workspaceId,
        getPostgresDestinationDefinitionId(),
        getDestinationDbConfig());
  }

  public DestinationRead createDestination(final String name,
                                           final UUID workspaceId,
                                           final UUID destinationDefId,
                                           final JsonNode destinationConfig)
      throws IOException {
    final DestinationRead destination = apiClient.getDestinationApi().createDestination(
        new DestinationCreate(
            workspaceId,
            name,
            destinationDefId,
            destinationConfig,
            null));
    destinationIds.add(destination.getDestinationId());
    return destination;
  }

  public DestinationRead updateDestination(final UUID destinationId, final JsonNode updatedConfig, final String name) throws IOException {
    final DestinationUpdate destinationUpdate = new DestinationUpdate(
        destinationId,
        updatedConfig,
        name,
        null);

    final CheckConnectionRead checkResponse = apiClient.getDestinationApi().checkConnectionToDestinationForUpdate(destinationUpdate);
    if (checkResponse.getStatus() != Status.SUCCEEDED) {
      throw new RuntimeException("Check connection failed: " + checkResponse.getMessage());
    }

    return Failsafe.with(retryPolicy).get(() -> apiClient.getDestinationApi().updateDestination(destinationUpdate));
  }

  public CheckConnectionRead.Status checkDestination(final UUID destinationId) throws IOException {
    return apiClient.getDestinationApi()
        .checkConnectionToDestination(new DestinationIdRequestBody(destinationId)).getStatus();
  }

  public OperationRead createDbtCloudWebhookOperation(final UUID workspaceId, final UUID webhookConfigId) throws Exception {
    return apiClient.getOperationApi().createOperation(
        new OperationCreate(
            workspaceId,
            "reqres test",
            new OperatorConfiguration(
                OperatorType.WEBHOOK,
                new OperatorWebhook(
                    webhookConfigId,
                    OperatorWebhook.WebhookType.DBT_CLOUD,
                    new OperatorWebhookDbtCloud(123, 456),
                    null,
                    null))));
  }

  public List<JsonNode> retrieveRecordsFromDatabase(final Database database, final String table) throws SQLException {
    return database.query(context -> context.fetch(String.format("SELECT * FROM %s;", table)))
        .stream()
        .map(Record::intoMap)
        .map(Jsons::jsonNode)
        .collect(Collectors.toList());
  }

  public JsonNode getSourceDbConfig() {
    return getDbConfig(sourcePsql, false, false, sourceDatabaseName);
  }

  public JsonNode getSourceDbConfigWithHiddenPassword() {
    return getDbConfig(sourcePsql, true, false, sourceDatabaseName);
  }

  public JsonNode getDestinationDbConfig() {
    return getDbConfig(destinationPsql, false, true, destinationDatabaseName);
  }

  public JsonNode getDestinationDbConfigWithHiddenPassword() {
    return getDbConfig(destinationPsql, true, true, destinationDatabaseName);
  }

  public JsonNode getDbConfig(final PostgreSQLContainer psql,
                              final boolean hiddenPassword,
                              final boolean withSchema,
                              final String databaseName) {
    try {
      final Map<Object, Object> dbConfig =
          (isKube && isGke) ? GKEPostgresConfig.dbConfig(
              hiddenPassword ? null : cloudSqlInstancePassword,
              withSchema,
              cloudSqlInstanceUsername,
              cloudSqlInstancePublicIp,
              databaseName) : localConfig(psql, hiddenPassword, withSchema);
      final var config = Jsons.jsonNode(dbConfig);
      LOGGER.info("Using db config: {}", Jsons.toPrettyString(config));
      return config;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map<Object, Object> localConfig(final PostgreSQLContainer psql,
                                          final boolean hiddenPassword,
                                          final boolean withSchema) {
    final Map<Object, Object> dbConfig = new HashMap<>();
    if (isCI && !isGke) {
      dbConfig.put(JdbcUtils.HOST_KEY, psql.getHost());
    } else {
      dbConfig.put(JdbcUtils.HOST_KEY, getHostname());
    }

    if (hiddenPassword) {
      dbConfig.put(JdbcUtils.PASSWORD_KEY, "**********");
    } else {
      dbConfig.put(JdbcUtils.PASSWORD_KEY, psql.getPassword());
    }

    dbConfig.put(JdbcUtils.PORT_KEY, psql.getFirstMappedPort());
    dbConfig.put(JdbcUtils.DATABASE_KEY, psql.getDatabaseName());
    dbConfig.put(JdbcUtils.USERNAME_KEY, psql.getUsername());
    dbConfig.put(JdbcUtils.SSL_KEY, false);

    if (withSchema) {
      dbConfig.put(JdbcUtils.SCHEMA_KEY, "public");
    }
    return dbConfig;
  }

  public String getHostname() {
    if (isKube) {
      if (!KUBE_PROCESS_RUNNER_HOST.equals("")) {
        return KUBE_PROCESS_RUNNER_HOST;
      }
      if (isMinikube) {
        // used with minikube driver=none instance
        try {
          return Inet4Address.getLocalHost().getHostAddress();
        } catch (final UnknownHostException e) {
          throw new RuntimeException(e);
        }
      } else {
        // used on a single node with docker driver
        return "host.docker.internal";
      }
    } else if (isMac) {
      return "host.docker.internal";
    } else {
      return "localhost";
    }
  }

  public JobInfoRead getJobInfoRead(final long id) throws IOException {
    return apiClient.getJobsApi().getJobInfo(new JobIdRequestBody(id));
  }

  public SourceDefinitionRead createE2eSourceDefinition(final UUID workspaceId) throws IOException {
    final var sourceDefinitionRead = apiClient.getSourceDefinitionApi().createCustomSourceDefinition(
        new CustomSourceDefinitionCreate(
            new SourceDefinitionCreate(
                "E2E Test Source",
                "airbyte/source-e2e-test",
                SOURCE_E2E_TEST_CONNECTOR_VERSION,
                URI.create("https://example.com"),
                null,
                null),
            workspaceId,
            null,
            null));
    sourceDefinitionIds.add(sourceDefinitionRead.getSourceDefinitionId());
    return sourceDefinitionRead;
  }

  public SourceDefinitionRead createPostgresSourceDefinition(final UUID workspaceId, final String dockerImageTag) throws IOException {
    final var sourceDefinitionRead = apiClient.getSourceDefinitionApi().createCustomSourceDefinition(
        new CustomSourceDefinitionCreate(
            new SourceDefinitionCreate(
                "Custom Postgres Source",
                "airbyte/source-postgres",
                dockerImageTag,
                URI.create("https://example.com"),
                null,
                null),
            workspaceId,
            null,
            null));
    sourceDefinitionIds.add(sourceDefinitionRead.getSourceDefinitionId());
    return sourceDefinitionRead;
  }

  public DestinationDefinitionRead createE2eDestinationDefinition(final UUID workspaceId) throws IOException {
    return Failsafe.with(retryPolicy).get(() -> apiClient.getDestinationDefinitionApi()
        .createCustomDestinationDefinition(
            new CustomDestinationDefinitionCreate(
                new DestinationDefinitionCreate(
                    "E2E Test Destination",
                    "airbyte/destination-e2e-test",
                    DESTINATION_E2E_TEST_CONNECTOR_VERSION,
                    URI.create("https://example.com"),
                    null,
                    null),
                workspaceId,
                null,
                null)));
  }

  public SourceRead createPostgresSource() throws IOException {
    return createPostgresSource(defaultWorkspaceId);
  }

  public SourceRead createPostgresSource(final UUID workspaceId) throws IOException {
    return createSource(
        "acceptanceTestDb-" + UUID.randomUUID(),
        workspaceId,
        getPostgresSourceDefinitionId(),
        getSourceDbConfig());
  }

  public SourceRead createSource(final String name, final UUID workspaceId, final UUID sourceDefId, final JsonNode sourceConfig) {
    final SourceRead source = Failsafe.with(retryPolicy).get(() -> apiClient.getSourceApi().createSource(
        new SourceCreate(
            sourceDefId,
            sourceConfig,
            workspaceId,
            name,
            null,
            null)));
    sourceIds.add(source.getSourceId());
    return source;
  }

  public SourceRead updateSource(final UUID sourceId, final JsonNode updatedConfig, final String name) throws IOException {
    final SourceUpdate sourceUpdate = new SourceUpdate(
        sourceId,
        updatedConfig,
        name,
        null,
        null);

    final CheckConnectionRead checkResponse = apiClient.getSourceApi().checkConnectionToSourceForUpdate(sourceUpdate);
    if (checkResponse.getStatus() != Status.SUCCEEDED) {
      throw new RuntimeException("Check connection failed: " + checkResponse.getMessage());
    }

    return Failsafe.with(retryPolicy).get(() -> apiClient.getSourceApi().updateSource(sourceUpdate));
  }

  public CheckConnectionRead checkSource(final UUID sourceId) {
    return Failsafe.with(retryPolicy).get(() -> apiClient.getSourceApi().checkConnectionToSource(new SourceIdRequestBody(sourceId)));
  }

  public UUID getPostgresSourceDefinitionId() {
    return Failsafe.with(retryPolicy).get(() -> apiClient.getSourceDefinitionApi().listSourceDefinitions().getSourceDefinitions()
        .stream()
        .filter(sourceRead -> "postgres".equalsIgnoreCase(sourceRead.getName()))
        .findFirst()
        .orElseThrow()
        .getSourceDefinitionId());
  }

  public UUID getPostgresDestinationDefinitionId() throws IOException {
    return getPostgresDestinationDefinition().getDestinationDefinitionId();
  }

  public DestinationDefinitionRead getPostgresDestinationDefinition() {
    return Failsafe.with(retryPolicy).get(() -> apiClient.getDestinationDefinitionApi().listDestinationDefinitions().getDestinationDefinitions()
        .stream()
        .filter(destRead -> "postgres".equalsIgnoreCase(destRead.getName()))
        .findFirst()
        .orElseThrow());
  }

  public void updateDestinationDefinitionVersion(final UUID destinationDefinitionId, final String dockerImageTag)
      throws IOException {
    Failsafe.with(retryPolicy).run(() -> apiClient.getDestinationDefinitionApi().updateDestinationDefinition(
        new DestinationDefinitionUpdate(
            destinationDefinitionId,
            dockerImageTag,
            defaultWorkspaceId,
            null,
            null)));
  }

  public void updateSourceDefinitionVersion(final UUID sourceDefinitionId, final String dockerImageTag) throws IOException {
    apiClient.getSourceDefinitionApi().updateSourceDefinition(
        new SourceDefinitionUpdate(
            sourceDefinitionId,
            dockerImageTag,
            defaultWorkspaceId,
            null,
            null));
  }

  private void clearSourceDbData() throws SQLException {
    final Database database = getSourceDatabase();
    final Set<SchemaTableNamePair> pairs = Databases.listAllTables(database);
    for (final SchemaTableNamePair pair : pairs) {
      LOGGER.debug("Clearing table {} {}", pair.schemaName(), pair.tableName());
      database.query(context -> context.execute(String.format("DROP TABLE %s.%s", pair.schemaName(), pair.tableName())));
    }
  }

  private void clearDestinationDbData() throws SQLException {
    final Database database = getDestinationDatabase();
    final Set<SchemaTableNamePair> pairs = Databases.listAllTables(database);
    for (final SchemaTableNamePair pair : pairs) {
      LOGGER.debug("Clearing table {} {}", pair.schemaName(), pair.tableName());
      database.query(context -> context.execute(String.format("DROP TABLE %s.%s CASCADE", pair.schemaName(), pair.tableName())));
    }
  }

  private void disableConnection(final UUID connectionId) throws Exception {
    final ConnectionUpdate connectionUpdate =
        new ConnectionUpdate(
            connectionId,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            ConnectionStatus.DEPRECATED,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    updateConnection(connectionUpdate);
  }

  private void deleteSource(final UUID sourceId) {
    Failsafe.with(retryPolicy).run(() -> apiClient.getSourceApi().deleteSource(new SourceIdRequestBody(sourceId)));
  }

  private void deleteDestination(final UUID destinationId) {
    Failsafe.with(retryPolicy).run(() -> apiClient.getDestinationApi().deleteDestination(new DestinationIdRequestBody(destinationId)));
  }

  private void deleteOperation(final UUID destinationId) {
    Failsafe.with(retryPolicy).run(() -> apiClient.getOperationApi().deleteOperation(new OperationIdRequestBody(destinationId)));
  }

  /**
   * Returns the most recent job for the provided connection.
   */
  public JobRead getMostRecentSyncForConnection(final UUID connectionId) {
    return Failsafe.with(retryPolicy).get(
        () -> apiClient.getJobsApi()
            .listJobsFor(
                new JobListRequestBody(List.of(JobConfigType.SYNC), connectionId.toString(),
                    null, null, null, null, null,
                    null, null, null, null))
            .getJobs()
            .stream().findFirst().map(JobWithAttemptsRead::getJob).orElseThrow());
  }

  public void waitForSuccessfulJob(final JobRead originalJob) throws InterruptedException, IOException {
    final JobRead job = waitWhileJobHasStatus(originalJob, Sets.newHashSet(JobStatus.PENDING, JobStatus.RUNNING, JobStatus.INCOMPLETE));

    final var debugInfo = new ArrayList<String>();

    if (!JobStatus.SUCCEEDED.equals(job.getStatus())) {
      // If a job failed during testing, show us why.
      final JobIdRequestBody id = new JobIdRequestBody(originalJob.getId());
      for (final AttemptInfoRead attemptInfo : apiClient.getJobsApi().getJobInfo(id).getAttempts()) {
        final var msg = "Unsuccessful job attempt " + attemptInfo.getAttempt().getId()
            + " with status " + job.getStatus() + " produced log output as follows: " + attemptInfo.getLogs().getLogLines();
        LOGGER.warn(msg);
        debugInfo.add(msg);
      }
    }
    assertEquals(JobStatus.SUCCEEDED, job.getStatus(), Strings.join(debugInfo, ", "));
    Thread.sleep(200);
  }

  public JobRead waitWhileJobHasStatus(final JobRead originalJob, final Set<JobStatus> jobStatuses)
      throws InterruptedException {
    return waitWhileJobHasStatus(originalJob, jobStatuses, Duration.ofMinutes(12));
  }

  @SuppressWarnings("BusyWait")
  public JobRead waitWhileJobHasStatus(final JobRead originalJob,
                                       final Set<JobStatus> jobStatuses,
                                       final Duration maxWaitTime)
      throws InterruptedException {
    JobRead job = originalJob;

    final Instant waitStart = Instant.now();
    while (jobStatuses.contains(job.getStatus())) {
      if (Duration.between(waitStart, Instant.now()).compareTo(maxWaitTime) > 0) {
        LOGGER.info("Max wait time of {} has been reached. Stopping wait.", maxWaitTime);
        break;
      }
      sleep(1000);
      try {
        job = apiClient.getJobsApi().getJobInfo(new JobIdRequestBody(job.getId())).getJob();
      } catch (final ClientException | ServerException | IOException e) {
        // TODO(mfsiega-airbyte): consolidate our polling/retrying logic.
        LOGGER.warn("error querying jobs api, retrying...");
      }
      LOGGER.info("waiting: job id: {} config type: {} status: {}", job.getId(), job.getConfigType(), job.getStatus());
    }
    return job;
  }

  @SuppressWarnings("BusyWait")
  public void waitWhileJobIsRunning(final JobRead job, final Duration maxWaitTime)
      throws IOException, InterruptedException {
    final Instant waitStart = Instant.now();
    JobDebugInfoRead jobDebugInfoRead = apiClient.getJobsApi().getJobDebugInfo(new JobIdRequestBody(job.getId()));
    LOGGER.info("workflow state: {}", jobDebugInfoRead.getWorkflowState());
    while (jobDebugInfoRead.getWorkflowState() != null && jobDebugInfoRead.getWorkflowState().getRunning()) {
      if (Duration.between(waitStart, Instant.now()).compareTo(maxWaitTime) > 0) {
        LOGGER.info("Max wait time of {} has been reached. Stopping wait.", maxWaitTime);
        break;
      }
      LOGGER.info("waiting: job id: {}, workflowState.isRunning is still true", job.getId());
      sleep(1000);
      jobDebugInfoRead = apiClient.getJobsApi().getJobDebugInfo(new JobIdRequestBody(job.getId()));
    }
  }

  @SuppressWarnings("BusyWait")
  public ConnectionState waitForConnectionState(final UUID connectionId)
      throws IOException, InterruptedException {
    ConnectionState connectionState =
        Failsafe.with(retryPolicy).get(() -> apiClient.getStateApi().getState(new ConnectionIdRequestBody(connectionId)));
    int count = 0;
    while (count < FINAL_INTERVAL_SECS && (connectionState.getState() == null || connectionState.getState().isNull())) {
      LOGGER.info("fetching connection state. attempt: {}", count++);
      connectionState = apiClient.getStateApi().getState(new ConnectionIdRequestBody(connectionId));
      sleep(1000);
    }
    return connectionState;
  }

  /**
   * Wait until the sync succeeds by polling the Jobs API.
   * <p>
   * NOTE: !!! THIS WILL POTENTIALLY POLL FOREVER !!! so make sure the calling code has a deadline;
   * for example, a test timeout.
   * <p>
   * TODO: re-work the collection of polling helpers we have here into a sane set that rely on test
   * timeouts instead of implementing their own deadline logic.
   */
  public void waitForSuccessfulSyncNoTimeout(final JobRead jobRead) throws Exception {
    var job = jobRead;
    while (IN_PROGRESS_JOB_STATUSES.contains(job.getStatus())) {
      job = getJobInfoRead(job.getId()).getJob();
      LOGGER.info("waiting: job id: {} config type: {} status: {}", job.getId(), job.getConfigType(), job.getStatus());
      sleep(3000);
    }
    assertEquals(JobStatus.SUCCEEDED, job.getStatus());
  }

  public JobRead waitUntilTheNextJobIsStarted(final UUID connectionId, final Long previousJobId) throws Exception {

    JobRead mostRecentSyncJob = getMostRecentSyncForConnection(connectionId);
    int count = 0;
    while (count < MAX_ALLOWED_SECOND_PER_RUN && mostRecentSyncJob.getId() == previousJobId) {
      Thread.sleep(Duration.ofSeconds(1).toMillis());
      mostRecentSyncJob = getMostRecentSyncForConnection(connectionId);
      ++count;
    }
    final boolean exceeded120seconds = count >= MAX_ALLOWED_SECOND_PER_RUN;
    if (exceeded120seconds) {
      // Fail because taking more than FINAL_INTERVAL_SECSseconds to start a job is not expected
      // Returning the current mostRecentSyncJob here could end up hiding some issues
      Assertions.fail("unable to find the next job within FINAL_INTERVAL_SECSseconds");
    }
    LOGGER.info("Time to run the job: " + count);
    return mostRecentSyncJob;
  }

  public void getNonExistentResource() throws IOException {
    apiClient.getDestinationDefinitionSpecificationApi()
        .getDestinationDefinitionSpecification(
            new DestinationDefinitionIdWithWorkspaceId(UUID.randomUUID(), UUID.randomUUID()));
  }

  public SourceDefinitionRead getSourceDefinition(final UUID sourceDefinitionId) throws IOException {
    return Failsafe.with(retryPolicy)
        .get(() -> apiClient.getSourceDefinitionApi().getSourceDefinition(new SourceDefinitionIdRequestBody(sourceDefinitionId)));
  }

  public ConnectionState getConnectionState(final UUID connectionId) {
    return Failsafe.with(retryPolicy).get(() -> apiClient.getStateApi().getState(new ConnectionIdRequestBody(connectionId)));
  }

  public void webBackendUpdateConnection(final WebBackendConnectionUpdate update) {
    Failsafe.with(retryPolicy).run(() -> apiClient.getWebBackendApi().webBackendUpdateConnection(update));
  }

  public List<JobWithAttemptsRead> listSyncsForWorkspaces(final List<UUID> workspaceIds) {
    return Failsafe.with(retryPolicy).get(() -> apiClient.getJobsApi().listJobsForWorkspaces(
        new JobListForWorkspacesRequestBody(List.of(JobConfigType.SYNC), null, workspaceIds, null, null, null, null, null, null, null, null))
        .getJobs());
  }

  public ConnectionReadList listAllConnectionsForWorkspace(final UUID workspaceId) {
    return Failsafe.with(retryPolicy)
        .get(() -> apiClient.getConnectionApi().listAllConnectionsForWorkspace(new WorkspaceIdRequestBody(workspaceId, false)));
  }

  public SourceReadList listSourcesForWorkspace(final UUID workspaceId) {
    return Failsafe.with(retryPolicy).get(() -> apiClient.getSourceApi().listSourcesForWorkspace(new WorkspaceIdRequestBody(workspaceId, false)));
  }

  public SourceReadList listSourcesForWorkspacePaginated(final List<UUID> workspaceIds) {
    return Failsafe.with(retryPolicy).get(() -> apiClient.getSourceApi().listSourcesForWorkspacePaginated(
        new ListResourcesForWorkspacesRequestBody(workspaceIds, new Pagination(1000, 0), null, null)));
  }

  public void deleteWorkspace(final UUID workspaceId) {
    Failsafe.with(retryPolicy).run(() -> apiClient.getWorkspaceApi().deleteWorkspace(new WorkspaceIdRequestBody(workspaceId, false)));
  }

  public void deleteSourceDefinition(final UUID sourceDefinitionId) {
    Failsafe.with(retryPolicy)
        .run(() -> apiClient.getSourceDefinitionApi().deleteSourceDefinition(new SourceDefinitionIdRequestBody(sourceDefinitionId)));
  }

  public void updateSchemaChangePreference(final UUID connectionId,
                                           final NonBreakingChangesPreference nonBreakingChangesPreference,
                                           final SchemaChangeBackfillPreference backfillPreference)
      throws IOException, InterruptedException {
    updateConnection(
        new ConnectionUpdate(
            connectionId,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            nonBreakingChangesPreference,
            backfillPreference,
            null,
            null));
  }

  public WebBackendConnectionRead webBackendGetConnectionAndRefreshSchema(final UUID connectionId) {
    return Failsafe.with(retryPolicy)
        .get(() -> apiClient.getWebBackendApi().webBackendGetConnection(new WebBackendConnectionRequestBody(connectionId, true)));
  }

  public void createWorkspaceWithId(final UUID workspaceId) {
    Failsafe.with(retryPolicy).run(() -> apiClient.getWorkspaceApi()
        .createWorkspaceIfNotExist(
            new WorkspaceCreateWithId(
                workspaceId,
                "Airbyte Acceptance Tests" + UUID.randomUUID(),
                DEFAULT_ORGANIZATION_ID,
                "acceptance-tests@airbyte.io",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null)));
  }

  public StreamStatusReadList getStreamStatuses(final UUID connectionId, final Long jobId, final Integer attempt, final UUID workspaceId) {
    return Failsafe.with(retryPolicy).get(() -> apiClient.getStreamStatusesApi().getStreamStatuses(
        new StreamStatusListRequestBody(
            new Pagination(100, 0),
            workspaceId,
            attempt,
            connectionId,
            jobId,
            null,
            null,
            null)));
  }

  public AirbyteCatalog setIncrementalAppendSyncMode(final AirbyteCatalog airbyteCatalog, final List<String> cursorField) {
    return new AirbyteCatalog(airbyteCatalog.getStreams().stream()
        .map(stream -> new AirbyteStreamAndConfiguration(stream.getStream(), new AirbyteStreamConfiguration(
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND,
            cursorField,
            null,
            stream.getConfig().getPrimaryKey(),
            stream.getConfig().getAliasName(),
            stream.getConfig().getSelected(),
            stream.getConfig().getSuggested(),
            stream.getConfig().getDestinationObjectName(),
            stream.getConfig().getIncludeFiles(),
            stream.getConfig().getFieldSelectionEnabled(),
            stream.getConfig().getSelectedFields(),
            stream.getConfig().getHashedFields(),
            stream.getConfig().getMappers(),
            stream.getConfig().getMinimumGenerationId(),
            stream.getConfig().getGenerationId(),
            stream.getConfig().getSyncId())))
        .collect(Collectors.toList()));
  }

  public WebBackendConnectionUpdate getUpdateInput(final ConnectionRead connection, final AirbyteCatalog catalog, final OperationRead operation) {
    setIncrementalAppendSyncMode(catalog, List.of(COLUMN_ID));

    return new WebBackendConnectionUpdate(
        connection.getConnectionId(),
        connection.getName(),
        connection.getNamespaceDefinition(),
        connection.getNamespaceFormat(),
        connection.getPrefix(),
        catalog,
        connection.getSchedule(),
        null,
        null,
        connection.getStatus(),
        null,
        false,
        List.of(new WebBackendOperationCreateOrUpdate(
            operation.getWorkspaceId(),
            operation.getName(),
            operation.getOperatorConfiguration(),
            operation.getOperationId())),
        connection.getSourceCatalogId(),
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public void compareCatalog(final AirbyteCatalog actual) {
    assertEquals(expectedAirbyteCatalog, actual);
  }

  private static String generateRandomCloudSqlDatabaseName() {
    return CLOUD_SQL_DATABASE_PREFIX + UUID.randomUUID();
  }

  public <T> TestFlagsSetter.FlagOverride<T> withFlag(final Flag<T> flag, final Context context, final T value) {
    return testFlagsSetter.withFlag(flag, value, context);
  }

  /**
   * Validates that job logs exist, are in the correct format and contain entries from various
   * participants. This method loops because not all participants may have reported by the time that a
   * job is marked as done.
   *
   * @param jobId The ID of the job associated with the job logs.
   * @param attemptNumber The attempt number of the job associated with the job logs.
   */
  public void validateLogs(final long jobId, final int attemptNumber) {
    final RetryPolicy<?> retryPolicy = RetryPolicy.builder()
        .handle(Exception.class, AssertionFailedError.class, org.opentest4j.AssertionFailedError.class)
        .withDelay(Duration.ofSeconds(5))
        .withMaxRetries(50)
        .build();
    try {
      Failsafe.with(retryPolicy).run(() -> {
        // Assert that job logs exist
        final var attempt = getApiClient().getAttemptApi().getAttemptForJob(
            new GetAttemptStatsRequestBody(jobId, attemptNumber));
        // Structured logs should exist
        assertEquals(LogFormatType.STRUCTURED, attempt.getLogType());
        assertFalse(attempt.getLogs().getEvents().isEmpty());
        assertTrue(attempt.getLogs().getLogLines().isEmpty());
        // Assert that certain log lines are present in job logs to verify that all components can
        // contribute
        final List<String> logLines = List.of(
            "APPLY Stage: CLAIM", // workload launcher
            "----- START REPLICATION -----" // container orchestrator
        );
        validateLogLines(logLines, attempt);
      });
    } catch (final FailsafeException e) {
      fail("Failed to validate logs: retries exceeded waiting for logs.", e);
    }
  }

  public void validateLogLines(final List<String> logLines, final AttemptInfoRead attempt) {
    logLines.forEach(logLine -> {
      if (attempt.getLogs().getEvents().stream().noneMatch(e -> e.getMessage().startsWith(logLine))) {
        fail("Job logs do not contain any lines that start with '" + logLine + "'.");
      }
    });
  }

}
