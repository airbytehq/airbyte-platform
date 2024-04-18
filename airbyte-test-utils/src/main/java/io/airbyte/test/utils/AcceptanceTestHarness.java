/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.WebBackendApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ActorDefinitionRequestBody;
import io.airbyte.api.client.model.generated.ActorType;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AirbyteStream;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.client.model.generated.AttemptInfoRead;
import io.airbyte.api.client.model.generated.CheckConnectionRead;
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
import io.airbyte.api.client.model.generated.DestinationCreate;
import io.airbyte.api.client.model.generated.DestinationDefinitionCreate;
import io.airbyte.api.client.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.client.model.generated.DestinationDefinitionRead;
import io.airbyte.api.client.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.client.model.generated.DestinationDefinitionUpdate;
import io.airbyte.api.client.model.generated.DestinationIdRequestBody;
import io.airbyte.api.client.model.generated.DestinationRead;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
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
import io.airbyte.api.client.model.generated.NamespaceDefinitionType;
import io.airbyte.api.client.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.client.model.generated.OperationCreate;
import io.airbyte.api.client.model.generated.OperationIdRequestBody;
import io.airbyte.api.client.model.generated.OperationRead;
import io.airbyte.api.client.model.generated.OperatorConfiguration;
import io.airbyte.api.client.model.generated.OperatorNormalization;
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
import io.airbyte.api.client.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.client.model.generated.StreamStatusReadList;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.api.client.model.generated.WebBackendConnectionRead;
import io.airbyte.api.client.model.generated.WebBackendConnectionRequestBody;
import io.airbyte.api.client.model.generated.WebBackendConnectionUpdate;
import io.airbyte.api.client.model.generated.WebBackendOperationCreateOrUpdate;
import io.airbyte.api.client.model.generated.WebhookConfigWrite;
import io.airbyte.api.client.model.generated.WorkspaceCreateWithId;
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.api.client.model.generated.WorkspaceUpdate;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.string.Strings;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.temporal.TemporalWorkflowUtils;
import io.airbyte.commons.temporal.config.TemporalSdkTimeouts;
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow;
import io.airbyte.commons.temporal.scheduling.state.WorkflowState;
import io.airbyte.commons.util.MoreProperties;
import io.airbyte.db.Database;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.test.container.AirbyteTestContainer;
import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.io.File;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.Assertions;
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
 * <li>{@link AirbyteTestContainer}</li>
 * <li>kubernetes client</li>
 * <li>lists of UUIDS representing IDs of sources, destinations, connections, and operations</li>
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class AcceptanceTestHarness {

  private static final Logger LOGGER = LoggerFactory.getLogger(AcceptanceTestHarness.class);

  private static final UUID DEFAULT_ORGANIZATION_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final String DOCKER_COMPOSE_FILE_NAME = "docker-compose.yaml";
  // assume env file is one directory level up from airbyte-tests.
  private static final File ENV_FILE = Path.of(System.getProperty("user.dir")).getParent().resolve(".env").toFile();

  private static final DockerImageName DESTINATION_POSTGRES_IMAGE_NAME = DockerImageName.parse("postgres:13-alpine");

  private static final DockerImageName SOURCE_POSTGRES_IMAGE_NAME = DockerImageName.parse("debezium/postgres:13-alpine")
      .asCompatibleSubstituteFor("postgres");

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

  private static boolean isKube;
  private static boolean isMinikube;
  private static boolean isGke;
  private static boolean isMac;
  private static boolean useExternalDeployment;
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

  private AirbyteTestContainer airbyteTestContainer;
  private AirbyteApiClient apiClient;

  private WebBackendApi webBackendApi;
  private final UUID defaultWorkspaceId;
  private final String postgresSqlInitFile;

  private final List<UUID> sourceIds = Lists.newArrayList();
  private final List<UUID> connectionIds = Lists.newArrayList();
  private final List<UUID> destinationIds = Lists.newArrayList();
  private final List<UUID> operationIds = Lists.newArrayList();
  private final List<UUID> sourceDefinitionIds = Lists.newArrayList();
  private DataSource sourceDataSource;
  private DataSource destinationDataSource;

  private String gcpProjectId;
  private String cloudSqlInstanceId;
  private String cloudSqlInstanceUsername;
  private String cloudSqlInstancePassword;
  private String cloudSqlInstancePrivateIp;
  private String cloudSqlInstancePublicIp;

  public void removeConnection(final UUID connection) {
    connectionIds.remove(connection);
  }

  public AcceptanceTestHarness(final AirbyteApiClient apiClient,
                               final WebBackendApi webBackendApi,
                               final UUID defaultWorkspaceId,
                               final String postgresSqlInitFile)
      throws URISyntaxException, IOException, InterruptedException, GeneralSecurityException {
    // reads env vars to assign static variables
    assignEnvVars();
    this.apiClient = apiClient;
    this.webBackendApi = webBackendApi;
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

    // by default use airbyte deployment governed by a test container.
    if (!useExternalDeployment) {
      LOGGER.info("Using deployment of airbyte managed by test containers.");
      airbyteTestContainer = new AirbyteTestContainer.Builder(new File(Resources.getResource(DOCKER_COMPOSE_FILE_NAME).toURI()))
          .setEnv(MoreProperties.envFileToProperties(ENV_FILE))
          // override env VERSION to use dev to test current build of airbyte.
          .setEnvVariable("VERSION", "dev")
          // override to use test mounts.
          .setEnvVariable("DATA_DOCKER_MOUNT", "airbyte_data_migration_test")
          .setEnvVariable("DB_DOCKER_MOUNT", "airbyte_db_migration_test")
          .setEnvVariable("WORKSPACE_DOCKER_MOUNT", "airbyte_workspace_migration_test")
          .setEnvVariable("LOCAL_ROOT", "/tmp/airbyte_local_migration_test")
          .setEnvVariable("LOCAL_DOCKER_MOUNT", "/tmp/airbyte_local_migration_test")
          .build();
      airbyteTestContainer.startBlocking();
    } else {
      LOGGER.info("Using external deployment of airbyte.");
    }
  }

  public AcceptanceTestHarness(final AirbyteApiClient apiClient, final WebBackendApi webBackendApi, final UUID defaultWorkspaceId)
      throws URISyntaxException, IOException, InterruptedException, GeneralSecurityException {
    this(apiClient, webBackendApi, defaultWorkspaceId, DEFAULT_POSTGRES_INIT_SQL_FILE);
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

    if (airbyteTestContainer != null) {
      airbyteTestContainer.stop();
    }
  }

  public void setup() throws SQLException, URISyntaxException, IOException, ApiException {
    if (isGke) {
      // Prepare the database data sources.
      LOGGER.info("postgresPassword: {}", cloudSqlInstancePassword);
      sourceDataSource = GKEPostgresConfig.getDataSource(
          cloudSqlInstanceUsername,
          cloudSqlInstancePassword,
          cloudSqlInstancePrivateIp,
          sourceDatabaseName);
      destinationDataSource = GKEPostgresConfig.getDataSource(
          cloudSqlInstanceUsername,
          cloudSqlInstancePassword,
          cloudSqlInstancePrivateIp,
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
        } catch (final ApiException e) {
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
              new ActorDefinitionRequestBody().actorDefinitionId(sourceDefinitionId).actorType(ActorType.SOURCE))
          .getConnections();

      final List<ConnectionRead> destinationDefinitionConnections = this.apiClient.getConnectionApi()
          .listConnectionsByActorDefinition(
              new ActorDefinitionRequestBody().actorDefinitionId(destinationDefinitionId).actorType(ActorType.DESTINATION))
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
    isMac = System.getProperty("os.name").startsWith("Mac");
    useExternalDeployment =
        System.getenv("USE_EXTERNAL_DEPLOYMENT") != null
            && System.getenv("USE_EXTERNAL_DEPLOYMENT").equalsIgnoreCase("true");
    ensureCleanSlate = System.getenv("ENSURE_CLEAN_SLATE") != null
        && System.getenv("ENSURE_CLEAN_SLATE").equalsIgnoreCase("true");
    gcpProjectId = System.getenv("GCP_PROJECT_ID");
    cloudSqlInstanceId = System.getenv("CLOUD_SQL_INSTANCE_ID");
    cloudSqlInstanceUsername = System.getenv("CLOUD_SQL_INSTANCE_USERNAME");
    cloudSqlInstancePassword = System.getenv("CLOUD_SQL_INSTANCE_PASSWORD");
    cloudSqlInstancePrivateIp = System.getenv("CLOUD_SQL_INSTANCE_PRIVATE_IP");
    cloudSqlInstancePublicIp = System.getenv("CLOUD_SQL_INSTANCE_PUBLIC_IP");
  }

  private WorkflowClient getWorkflowClient() {
    final TemporalUtils temporalUtils = new TemporalUtils(null, null, null, null, null, null, null);
    final WorkflowServiceStubs temporalService = temporalUtils.createTemporalService(
        TemporalWorkflowUtils.getAirbyteTemporalOptions("localhost:7233", new TemporalSdkTimeouts()),
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

  public AirbyteCatalog discoverSourceSchema(final UUID sourceId) {
    return discoverSourceSchemaWithId(sourceId).getCatalog();
  }

  public SourceDiscoverSchemaRead discoverSourceSchemaWithId(final UUID sourceId) {
    return AirbyteApiClient.retryWithJitter(
        () -> {
          final var result =
              apiClient.getSourceApi().discoverSchemaForSource(new SourceDiscoverSchemaRequestBody().sourceId(sourceId).disableCache(true));
          if (result.getCatalog() == null) {
            throw new RuntimeException("no catalog returned, retrying...");
          }
          return result;
        },
        "discover source schema", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  // Run check Connection workflow.
  public void checkConnection(final UUID sourceId) throws ApiException {
    apiClient.getSourceApi().checkConnectionToSource(new SourceIdRequestBody().sourceId(sourceId));
  }

  public AirbyteCatalog discoverSourceSchemaWithoutCache(final UUID sourceId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getSourceApi().discoverSchemaForSource(
        new SourceDiscoverSchemaRequestBody().sourceId(sourceId).disableCache(true)).getCatalog(), "discover source schema no cache",
        JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public DestinationDefinitionSpecificationRead getDestinationDefinitionSpec(final UUID destinationDefinitionId, final UUID workspaceId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getDestinationDefinitionSpecificationApi()
        .getDestinationDefinitionSpecification(
            new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(destinationDefinitionId).workspaceId(workspaceId)),
        "get destination definition spec", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public SourceDefinitionSpecificationRead getSourceDefinitionSpec(final UUID sourceDefinitionId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getSourceDefinitionSpecificationApi()
        .getSourceDefinitionSpecification(
            new SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(sourceDefinitionId).workspaceId(UUID.randomUUID())),
        "get source definition spec", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
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

    return createConnectionFromRequest(new ConnectionCreate()
        .status(ConnectionStatus.ACTIVE)
        .sourceId(create.getSrcId())
        .destinationId(create.getDstId())
        .syncCatalog(create.getConfiguredCatalog())
        .sourceCatalogId(create.getCatalogId())
        .scheduleType(create.getScheduleType())
        .scheduleData(create.getScheduleData())
        .operationIds(create.getOperationIds())
        .name(name)
        .namespaceDefinition(NamespaceDefinitionType.CUSTOMFORMAT)
        .namespaceFormat(namespace)
        .prefix(OUTPUT_STREAM_PREFIX)
        .geography(create.getGeography()));
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
        new ConnectionCreate()
            .status(ConnectionStatus.ACTIVE)
            .sourceId(create.getSrcId())
            .destinationId(create.getDstId())
            .syncCatalog(create.getConfiguredCatalog())
            .sourceCatalogId(create.getCatalogId())
            .scheduleType(create.getScheduleType())
            .scheduleData(create.getScheduleData())
            .operationIds(create.getOperationIds())
            .name(name)
            .namespaceDefinition(NamespaceDefinitionType.CUSTOMFORMAT)
            .namespaceFormat(namespace + "_${SOURCE_NAMESPACE}")
            .prefix(OUTPUT_STREAM_PREFIX)
            .geography(create.getGeography()));
  }

  private ConnectionRead createConnectionFromRequest(final ConnectionCreate request) throws Exception {
    final ConnectionRead connection = AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getConnectionApi().createConnection(request),
        "create connection", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
    connectionIds.add(connection.getConnectionId());
    return connection;
  }

  public ConnectionRead getConnection(final UUID connectionId) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(
        () -> apiClient.getConnectionApi().getConnection(new ConnectionIdRequestBody().connectionId(connectionId)), "get connection",
        JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public void updateConnectionSchedule(
                                       final UUID connectionId,
                                       final ConnectionScheduleType newScheduleType,
                                       final ConnectionScheduleData newScheduleData)
      throws Exception {
    updateConnection(
        new ConnectionUpdate()
            .connectionId(connectionId)
            .scheduleType(newScheduleType)
            .scheduleData(newScheduleData));
  }

  public void updateConnectionCatalog(final UUID connectionId, final AirbyteCatalog catalog) throws Exception {
    updateConnection(new ConnectionUpdate()
        .connectionId(connectionId)
        .syncCatalog(catalog));
  }

  public ConnectionRead updateConnectionSourceCatalogId(final UUID connectionId, UUID sourceCatalogId) throws Exception {
    return updateConnection(
        new ConnectionUpdate()
            .connectionId(connectionId)
            .sourceCatalogId(sourceCatalogId));
  }

  private ConnectionRead updateConnection(final ConnectionUpdate request) throws Exception {
    final var result = AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getConnectionApi().updateConnection(request),
        "update connection catalog", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
    // Attempting to sync immediately after updating the connection can run into a race condition in the
    // connection manager workflow hangs. This should be fixed in the backend, but for now we try to
    // tolerate it.
    Thread.sleep(1000 * 5);
    return result;
  }

  public JobInfoRead syncConnection(final UUID connectionId) {
    return AirbyteApiClient.retryWithJitter(
        () -> apiClient.getConnectionApi().syncConnection(new ConnectionIdRequestBody().connectionId(connectionId)),
        "sync connection", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public JobInfoRead cancelSync(final long jobId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getJobsApi().cancelJob(new JobIdRequestBody().id(jobId)),
        "cancel sync job", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public JobInfoRead resetConnection(final UUID connectionId) {
    return AirbyteApiClient.retryWithJitter(
        () -> apiClient.getConnectionApi().resetConnection(new ConnectionIdRequestBody().connectionId(connectionId)),
        "reset connection", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public void deleteConnection(final UUID connectionId) {
    AirbyteApiClient.retryWithJitter(() -> {
      apiClient.getConnectionApi().deleteConnection(new ConnectionIdRequestBody().connectionId(connectionId));
      return null;
    }, "delete connection", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public DestinationRead createPostgresDestination() {
    return createPostgresDestination(defaultWorkspaceId);
  }

  public DestinationRead createPostgresDestination(final UUID workspaceId) {
    return createDestination(
        "AccTestDestination-" + UUID.randomUUID(),
        workspaceId,
        getPostgresDestinationDefinitionId(),
        getDestinationDbConfig());
  }

  public DestinationRead createDestination(final String name,
                                           final UUID workspaceId,
                                           final UUID destinationDefId,
                                           final JsonNode destinationConfig) {
    final DestinationRead destination =
        AirbyteApiClient.retryWithJitter(() -> apiClient.getDestinationApi().createDestination(new DestinationCreate()
            .name(name)
            .connectionConfiguration(Jsons.jsonNode(destinationConfig))
            .workspaceId(workspaceId)
            .destinationDefinitionId(destinationDefId)), "create destination", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
    destinationIds.add(destination.getDestinationId());
    return destination;
  }

  public CheckConnectionRead.StatusEnum checkDestination(final UUID destinationId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getDestinationApi()
        .checkConnectionToDestination(new DestinationIdRequestBody().destinationId(destinationId))
        .getStatus(), "check connection", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public OperationRead createNormalizationOperation() {
    return createNormalizationOperation(defaultWorkspaceId);
  }

  public OperationRead createNormalizationOperation(final UUID workspaceId) {
    final OperatorConfiguration normalizationConfig = new OperatorConfiguration()
        .operatorType(OperatorType.NORMALIZATION).normalization(new OperatorNormalization().option(
            OperatorNormalization.OptionEnum.BASIC));

    final OperationCreate operationCreate = new OperationCreate()
        .workspaceId(workspaceId)
        .name("AccTestDestination-" + UUID.randomUUID()).operatorConfiguration(normalizationConfig);

    final OperationRead operation = AirbyteApiClient.retryWithJitter(() -> apiClient.getOperationApi().createOperation(operationCreate),
        "create operation", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
    operationIds.add(operation.getOperationId());
    return operation;
  }

  public OperationRead createDbtCloudWebhookOperation(final UUID workspaceId, final UUID webhookConfigId) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getOperationApi().createOperation(new OperationCreate()
        .workspaceId(workspaceId)
        .name("reqres test")
        .operatorConfiguration(new OperatorConfiguration()
            .operatorType(OperatorType.WEBHOOK)
            .webhook(new OperatorWebhook()
                .webhookConfigId(webhookConfigId)
                // NOTE: this dbt Cloud config won't actually work, but the sync should still succeed.
                .webhookType(OperatorWebhook.WebhookTypeEnum.DBTCLOUD)
                .dbtCloud(new OperatorWebhookDbtCloud().accountId(123).jobId(456))))),
        "create dbt cloud operation", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
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
    dbConfig.put(JdbcUtils.HOST_KEY, getHostname());

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

  public JobInfoRead getJobInfoRead(final long id) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(
        () -> apiClient.getJobsApi().getJobInfo(new JobIdRequestBody().id(id)),
        "get job info", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public SourceDefinitionRead createE2eSourceDefinition(final UUID workspaceId) {
    final var sourceDefinitionRead = AirbyteApiClient.retryWithJitter(
        () -> apiClient.getSourceDefinitionApi().createCustomSourceDefinition(new CustomSourceDefinitionCreate()
            .workspaceId(workspaceId)
            .sourceDefinition(new SourceDefinitionCreate()
                .name("E2E Test Source")
                .dockerRepository("airbyte/source-e2e-test")
                .dockerImageTag(SOURCE_E2E_TEST_CONNECTOR_VERSION)
                .documentationUrl(URI.create("https://example.com")))),
        "create customer source definition", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
    sourceDefinitionIds.add(sourceDefinitionRead.getSourceDefinitionId());
    return sourceDefinitionRead;
  }

  public SourceDefinitionRead createPostgresSourceDefinition(final UUID workspaceId, final String dockerImageTag) throws Exception {
    final var sourceDefinitionRead = AirbyteApiClient.retryWithJitterThrows(
        () -> apiClient.getSourceDefinitionApi().createCustomSourceDefinition(new CustomSourceDefinitionCreate()
            .workspaceId(workspaceId)
            .sourceDefinition(new SourceDefinitionCreate()
                .name("Custom Postgres Source")
                .dockerRepository("airbyte/source-postgres")
                .dockerImageTag(dockerImageTag)
                .documentationUrl(URI.create("https://example.com")))),
        "create customer source definition", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
    sourceDefinitionIds.add(sourceDefinitionRead.getSourceDefinitionId());
    return sourceDefinitionRead;
  }

  public DestinationDefinitionRead createE2eDestinationDefinition(final UUID workspaceId) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getDestinationDefinitionApi()
        .createCustomDestinationDefinition(new CustomDestinationDefinitionCreate()
            .workspaceId(workspaceId)
            .destinationDefinition(new DestinationDefinitionCreate()
                .name("E2E Test Destination")
                .dockerRepository("airbyte/destination-e2e-test")
                .dockerImageTag(DESTINATION_E2E_TEST_CONNECTOR_VERSION)
                .documentationUrl(URI.create("https://example.com")))),
        "create destination definition", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public SourceRead createPostgresSource() {
    return createPostgresSource(defaultWorkspaceId);
  }

  public SourceRead createPostgresSource(final UUID workspaceId) {
    return createSource(
        "acceptanceTestDb-" + UUID.randomUUID(),
        workspaceId,
        getPostgresSourceDefinitionId(),
        getSourceDbConfig());
  }

  public SourceRead createSource(final String name, final UUID workspaceId, final UUID sourceDefId, final JsonNode sourceConfig) {
    final SourceRead source = AirbyteApiClient.retryWithJitter(() -> apiClient.getSourceApi().createSource(new SourceCreate()
        .name(name)
        .sourceDefinitionId(sourceDefId)
        .workspaceId(workspaceId)
        .connectionConfiguration(sourceConfig)), "create source", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
    sourceIds.add(source.getSourceId());
    return source;
  }

  public CheckConnectionRead checkSource(final UUID sourceId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getSourceApi().checkConnectionToSource(new SourceIdRequestBody().sourceId(sourceId)),
        "check source", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public UUID getPostgresSourceDefinitionId() {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getSourceDefinitionApi().listSourceDefinitions().getSourceDefinitions()
        .stream()
        .filter(sourceRead -> "postgres".equalsIgnoreCase(sourceRead.getName()))
        .findFirst()
        .orElseThrow()
        .getSourceDefinitionId(), "get postgres definition", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public UUID getPostgresDestinationDefinitionId() {
    return getPostgresDestinationDefinition().getDestinationDefinitionId();
  }

  public DestinationDefinitionRead getPostgresDestinationDefinition() {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getDestinationDefinitionApi().listDestinationDefinitions().getDestinationDefinitions()
        .stream()
        .filter(destRead -> "postgres".equalsIgnoreCase(destRead.getName()))
        .findFirst()
        .orElseThrow(), "get postgres definition", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public void updateDestinationDefinitionVersion(final UUID destinationDefinitionId, final String dockerImageTag) throws ApiException {
    apiClient.getDestinationDefinitionApi().updateDestinationDefinition(new DestinationDefinitionUpdate()
        .destinationDefinitionId(destinationDefinitionId).dockerImageTag(dockerImageTag));
  }

  public void updateSourceDefinitionVersion(final UUID sourceDefinitionId, final String dockerImageTag) throws ApiException {
    apiClient.getSourceDefinitionApi().updateSourceDefinition(new SourceDefinitionUpdate()
        .sourceDefinitionId(sourceDefinitionId).dockerImageTag(dockerImageTag));
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
        new ConnectionUpdate().connectionId(connectionId).status(ConnectionStatus.DEPRECATED);
    updateConnection(connectionUpdate);
  }

  private void deleteSource(final UUID sourceId) {
    AirbyteApiClient.retryWithJitter(() -> {
      apiClient.getSourceApi().deleteSource(new SourceIdRequestBody().sourceId(sourceId));
      return null; // Note: the retryWithJitter needs a return.
    }, "delete source", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  private void deleteDestination(final UUID destinationId) {
    AirbyteApiClient.retryWithJitter(() -> {
      apiClient.getDestinationApi().deleteDestination(new DestinationIdRequestBody().destinationId(destinationId));
      return null; // Note: the retryWithJitter needs a return.
    }, "delete destination", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  private void deleteOperation(final UUID destinationId) {
    AirbyteApiClient.retryWithJitter(() -> {
      apiClient.getOperationApi().deleteOperation(new OperationIdRequestBody().operationId(destinationId));
      return null;
    }, "delete operation", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  /**
   * Returns the most recent job for the provided connection.
   */
  public JobRead getMostRecentSyncForConnection(final UUID connectionId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getJobsApi()
        .listJobsFor(new JobListRequestBody().configId(connectionId.toString()).configTypes(List.of(JobConfigType.SYNC)))
        .getJobs()
        .stream().findFirst().map(JobWithAttemptsRead::getJob).orElseThrow(), "get most recent sync job", JITTER_MAX_INTERVAL_SECS,
        FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public void waitForSuccessfulJob(final JobRead originalJob) throws InterruptedException, ApiException {
    final JobRead job = waitWhileJobHasStatus(originalJob, Sets.newHashSet(JobStatus.PENDING, JobStatus.RUNNING, JobStatus.INCOMPLETE));

    final var debugInfo = new ArrayList<String>();

    if (!JobStatus.SUCCEEDED.equals(job.getStatus())) {
      // If a job failed during testing, show us why.
      final JobIdRequestBody id = new JobIdRequestBody();
      id.setId(originalJob.getId());
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
        job = apiClient.getJobsApi().getJobInfo(new JobIdRequestBody().id(job.getId())).getJob();
      } catch (final ApiException e) {
        // TODO(mfsiega-airbyte): consolidate our polling/retrying logic.
        LOGGER.warn("error querying jobs api, retrying...");
      }
      LOGGER.info("waiting: job id: {} config type: {} status: {}", job.getId(), job.getConfigType(), job.getStatus());
    }
    return job;
  }

  @SuppressWarnings("BusyWait")
  public void waitWhileJobIsRunning(final JobRead job, final Duration maxWaitTime)
      throws ApiException, InterruptedException {
    final Instant waitStart = Instant.now();
    JobDebugInfoRead jobDebugInfoRead = apiClient.getJobsApi().getJobDebugInfo(new JobIdRequestBody().id(job.getId()));
    LOGGER.info("workflow state: {}", jobDebugInfoRead.getWorkflowState());
    while (jobDebugInfoRead.getWorkflowState() != null && jobDebugInfoRead.getWorkflowState().getRunning()) {
      if (Duration.between(waitStart, Instant.now()).compareTo(maxWaitTime) > 0) {
        LOGGER.info("Max wait time of {} has been reached. Stopping wait.", maxWaitTime);
        break;
      }
      LOGGER.info("waiting: job id: {}, workflowState.isRunning is still true", job.getId());
      sleep(1000);
      jobDebugInfoRead = apiClient.getJobsApi().getJobDebugInfo(new JobIdRequestBody().id(job.getId()));
    }
  }

  @SuppressWarnings("BusyWait")
  public ConnectionState waitForConnectionState(final UUID connectionId)
      throws ApiException, InterruptedException {
    ConnectionState connectionState = AirbyteApiClient.retryWithJitter(
        () -> apiClient.getStateApi().getState(new ConnectionIdRequestBody().connectionId(connectionId)), "get connection state",
        JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
    int count = 0;
    while (count < FINAL_INTERVAL_SECS && (connectionState.getState() == null || connectionState.getState().isNull())) {
      LOGGER.info("fetching connection state. attempt: {}", count++);
      connectionState = apiClient.getStateApi().getState(new ConnectionIdRequestBody().connectionId(connectionId));
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

  public JobRead waitUntilTheNextJobIsStarted(final UUID connectionId, Long previousJobId) throws Exception {

    JobRead mostRecentSyncJob = getMostRecentSyncForConnection(connectionId);
    int count = 0;
    while (count < MAX_ALLOWED_SECOND_PER_RUN && mostRecentSyncJob.getId().equals(previousJobId)) {
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

  public void getNonExistentResource() throws ApiException {
    apiClient.getDestinationDefinitionSpecificationApi()
        .getDestinationDefinitionSpecification(
            new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(UUID.randomUUID()).workspaceId(UUID.randomUUID()));
  }

  public WorkspaceRead updateWorkspaceWebhookConfigs(UUID workspaceId, List<WebhookConfigWrite> webhookConfigs) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getWorkspaceApi().updateWorkspace(new WorkspaceUpdate()
        .workspaceId(workspaceId)
        .webhookConfigs(webhookConfigs)), "update workspace webhook configs", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public SourceDefinitionRead getSourceDefinition(UUID sourceDefinitionId) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getSourceDefinitionApi().getSourceDefinition(
        new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinitionId)),
        "get source definition", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public ConnectionState getConnectionState(UUID connectionId) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getStateApi().getState(
        new ConnectionIdRequestBody().connectionId(connectionId)),
        "get state", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public void webBackendUpdateConnection(WebBackendConnectionUpdate update) throws Exception {
    AirbyteApiClient.retryWithJitterThrows(() -> webBackendApi.webBackendUpdateConnection(update), "web backend update", JITTER_MAX_INTERVAL_SECS,
        FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public List<JobWithAttemptsRead> listSyncsForWorkspaces(List<UUID> workspaceIds) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getJobsApi().listJobsForWorkspaces(
        new JobListForWorkspacesRequestBody()
            .workspaceIds(workspaceIds)
            .addConfigTypesItem(JobConfigType.SYNC)),
        "list jobs for workspaces", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES)
        .getJobs();
  }

  public ConnectionReadList listAllConnectionsForWorkspace(UUID workspaceId) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getConnectionApi().listAllConnectionsForWorkspace(
        new WorkspaceIdRequestBody().workspaceId(workspaceId)), "list all connections for workspace", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS,
        MAX_TRIES);
  }

  public SourceReadList listSourcesForWorkspace(UUID workspaceId) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getSourceApi().listSourcesForWorkspace(
        new WorkspaceIdRequestBody().workspaceId(workspaceId)), "list sources", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public SourceReadList listSourcesForWorkspacePaginated(List<UUID> workspaceIds) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getSourceApi().listSourcesForWorkspacePaginated(
        new ListResourcesForWorkspacesRequestBody().workspaceIds(workspaceIds)
            .pagination(new Pagination().pageSize(1000).rowOffset(0))),
        "list sources", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public void deleteWorkspace(UUID workspaceId) throws Exception {
    AirbyteApiClient.retryWithJitterThrows(() -> {
      apiClient.getWorkspaceApi().deleteWorkspace(new io.airbyte.api.client.model.generated.WorkspaceIdRequestBody().workspaceId(workspaceId));
      return null;
    },
        "list sources", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public void deleteSourceDefinition(UUID sourceDefinitionId) throws Exception {
    AirbyteApiClient.retryWithJitterThrows(() -> {
      apiClient.getSourceDefinitionApi().deleteSourceDefinition(new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinitionId));
      return null;
    }, "delete source definition", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public void updateSchemaChangePreference(final UUID connectionId,
                                           final NonBreakingChangesPreference nonBreakingChangesPreference,
                                           final SchemaChangeBackfillPreference backfillPreference)
      throws Exception {
    updateConnection(
        new ConnectionUpdate()
            .connectionId(connectionId)
            .nonBreakingChangesPreference(nonBreakingChangesPreference)
            .backfillPreference(backfillPreference));
  }

  public WebBackendConnectionRead webBackendGetConnectionAndRefreshSchema(UUID connectionId) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(() -> webBackendApi.webBackendGetConnection(
        new WebBackendConnectionRequestBody().connectionId(connectionId).withRefreshedCatalog(true)),
        "get connection and refresh schema", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public void createWorkspaceWithId(UUID workspaceId) throws Exception {
    AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getWorkspaceApi()
        .createWorkspaceIfNotExist(new WorkspaceCreateWithId()
            .id(workspaceId)
            .email("acceptance-tests@airbyte.io")
            .name("Airbyte Acceptance Tests" + UUID.randomUUID()).organizationId(DEFAULT_ORGANIZATION_ID)),
        "create workspace", 10, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public StreamStatusReadList getStreamStatuses(UUID connectionId, Long jobId, Integer attempt, UUID workspaceId) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getStreamStatusesApi().getStreamStatuses(new StreamStatusListRequestBody()
        .connectionId(connectionId)
        .jobId(jobId)
        .attemptNumber(attempt)
        .workspaceId(workspaceId)
        .pagination(new Pagination().pageSize(100).rowOffset(0))),
        "get stream statuses", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public void setIncrementalAppendSyncMode(final AirbyteCatalog airbyteCatalog, final List<String> cursorField) {
    airbyteCatalog.getStreams().forEach(stream -> {
      stream.getConfig().syncMode(SyncMode.INCREMENTAL)
          .destinationSyncMode(DestinationSyncMode.APPEND)
          .cursorField(cursorField);
    });
  }

  public WebBackendConnectionUpdate getUpdateInput(final ConnectionRead connection, final AirbyteCatalog catalog, final OperationRead operation) {
    setIncrementalAppendSyncMode(catalog, List.of(COLUMN_ID));

    return new WebBackendConnectionUpdate()
        .connectionId(connection.getConnectionId())
        .name(connection.getName())
        .operations(List.of(new WebBackendOperationCreateOrUpdate()
            .name(operation.getName())
            .operationId(operation.getOperationId())
            .workspaceId(operation.getWorkspaceId())
            .operatorConfiguration(operation.getOperatorConfiguration())))
        .namespaceDefinition(connection.getNamespaceDefinition())
        .namespaceFormat(connection.getNamespaceFormat())
        .syncCatalog(catalog)
        .schedule(connection.getSchedule())
        .sourceCatalogId(connection.getSourceCatalogId())
        .status(connection.getStatus())
        .prefix(connection.getPrefix())
        .skipReset(false);
  }

  public void compareCatalog(AirbyteCatalog actual) {
    final JsonNode expectedSchema = Jsons.deserialize("""
                                                      {
                                                        "type": "object",
                                                        "properties": {
                                                          "%s": {
                                                            "type": "number",
                                                            "airbyte_type": "integer"
                                                          },
                                                          "%s": {
                                                            "type": "string"
                                                          }
                                                        }
                                                      }
                                                      """.formatted(COLUMN_ID, COLUMN_NAME));
    final AirbyteStream expectedStream = new AirbyteStream()
        .name(STREAM_NAME)
        .namespace(PUBLIC)
        .jsonSchema(expectedSchema)
        .sourceDefinedCursor(null)
        .defaultCursorField(Collections.emptyList())
        .sourceDefinedPrimaryKey(Collections.emptyList())
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
    final AirbyteStreamConfiguration expectedStreamConfig = new AirbyteStreamConfiguration()
        .syncMode(SyncMode.FULL_REFRESH)
        .cursorField(Collections.emptyList())
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .primaryKey(Collections.emptyList())
        .aliasName(STREAM_NAME.replace(".", "_"))
        .selected(true)
        .suggested(true);
    final AirbyteCatalog expected = new AirbyteCatalog()
        .streams(Lists.newArrayList(new AirbyteStreamAndConfiguration()
            .stream(expectedStream)
            .config(expectedStreamConfig)));

    assertEquals(expected, actual);
  }

  private static String generateRandomCloudSqlDatabaseName() {
    return CLOUD_SQL_DATABASE_PREFIX + UUID.randomUUID();
  }

}
