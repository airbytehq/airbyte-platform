/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ActorDefinitionRequestBody;
import io.airbyte.api.client.model.generated.ActorType;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AttemptInfoRead;
import io.airbyte.api.client.model.generated.CheckConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionCreate;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
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
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.api.client.model.generated.JobConfigType;
import io.airbyte.api.client.model.generated.JobDebugInfoRead;
import io.airbyte.api.client.model.generated.JobIdRequestBody;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobListRequestBody;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.JobStatus;
import io.airbyte.api.client.model.generated.JobWithAttemptsRead;
import io.airbyte.api.client.model.generated.NamespaceDefinitionType;
import io.airbyte.api.client.model.generated.OperationCreate;
import io.airbyte.api.client.model.generated.OperationIdRequestBody;
import io.airbyte.api.client.model.generated.OperationRead;
import io.airbyte.api.client.model.generated.OperatorConfiguration;
import io.airbyte.api.client.model.generated.OperatorNormalization;
import io.airbyte.api.client.model.generated.OperatorType;
import io.airbyte.api.client.model.generated.Pagination;
import io.airbyte.api.client.model.generated.SourceCreate;
import io.airbyte.api.client.model.generated.SourceDefinitionCreate;
import io.airbyte.api.client.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.client.model.generated.SourceDefinitionRead;
import io.airbyte.api.client.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.api.client.model.generated.SourceDefinitionUpdate;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRequestBody;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.api.client.model.generated.SourceRead;
import io.airbyte.api.client.model.generated.StreamStatusJobType;
import io.airbyte.api.client.model.generated.StreamStatusListRequestBody;
import io.airbyte.api.client.model.generated.StreamStatusRead;
import io.airbyte.api.client.model.generated.StreamStatusReadList;
import io.airbyte.api.client.model.generated.StreamStatusRunState;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.api.client.model.generated.WebBackendConnectionUpdate;
import io.airbyte.api.client.model.generated.WebBackendOperationCreateOrUpdate;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.temporal.TemporalWorkflowUtils;
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow;
import io.airbyte.commons.temporal.scheduling.state.WorkflowState;
import io.airbyte.commons.util.MoreProperties;
import io.airbyte.db.Database;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.test.container.AirbyteTestContainer;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
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
@SuppressWarnings("MissingJavadocMethod")
public class AirbyteAcceptanceTestHarness {

  private static final Logger LOGGER = LoggerFactory.getLogger(AirbyteAcceptanceTestHarness.class);

  private static final String DOCKER_COMPOSE_FILE_NAME = "docker-compose.yaml";
  // assume env file is one directory level up from airbyte-tests.
  private static final File ENV_FILE = Path.of(System.getProperty("user.dir")).getParent().resolve(".env").toFile();

  private static final DockerImageName DESTINATION_POSTGRES_IMAGE_NAME = DockerImageName.parse("postgres:13-alpine");

  private static final DockerImageName SOURCE_POSTGRES_IMAGE_NAME = DockerImageName.parse("debezium/postgres:13-alpine")
      .asCompatibleSubstituteFor("postgres");

  private static final String SOURCE_E2E_TEST_CONNECTOR_VERSION = "0.1.2";
  private static final String DESTINATION_E2E_TEST_CONNECTOR_VERSION = "0.1.1";

  public static final String POSTGRES_SOURCE_LEGACY_CONNECTOR_VERSION = "0.4.26";

  private static final String OUTPUT_NAMESPACE_PREFIX = "output_namespace_";
  private static final String OUTPUT_NAMESPACE = OUTPUT_NAMESPACE_PREFIX + "${SOURCE_NAMESPACE}";
  private static final String OUTPUT_STREAM_PREFIX = "output_table_";
  private static final String TABLE_NAME = "id_and_name";
  public static final String STREAM_NAME = TABLE_NAME;
  public static final String COLUMN_ID = "id";
  public static final String COLUMN_NAME = "name";
  private static final String COLUMN_NAME_DATA = "_airbyte_data";
  private static final String SOURCE_USERNAME = "sourceusername";
  public static final String SOURCE_PASSWORD = "hunter2";
  public static final String PUBLIC_SCHEMA_NAME = "public";
  public static final String STAGING_SCHEMA_NAME = "staging";
  public static final String COOL_EMPLOYEES_TABLE_NAME = "cool_employees";
  public static final String AWESOME_PEOPLE_TABLE_NAME = "awesome_people";

  private static final String DEFAULT_POSTGRES_INIT_SQL_FILE = "postgres_init.sql";
  private static final String ECHO_SERVER_IMAGE = "mendhak/http-https-echo:29";

  // Used for bypassing SSL modification for db configs
  private static final String IS_TEST = "is_test";
  public static final int JITTER_MAX_INTERVAL_SECS = 10;
  public static final int FINAL_INTERVAL_SECS = 60;
  public static final int MAX_TRIES = 3;
  public static final int MAX_ALLOWED_SECOND_PER_RUN = 120;

  // NOTE: we include `INCOMPLETE` here because the job may still retry; see
  // https://docs.airbyte.com/understanding-airbyte/jobs/.
  public static final Set<JobStatus> IN_PROGRESS_JOB_STATUSES = Set.of(JobStatus.PENDING, JobStatus.INCOMPLETE, JobStatus.RUNNING);

  private static boolean isKube;
  private static boolean isMinikube;
  private static boolean isGke;
  private static boolean isMac;
  private static boolean useExternalDeployment;
  private static boolean ensureCleanSlate;

  /**
   * When the acceptance tests are run against a local instance of docker-compose or KUBE then these
   * test containers are used. When we run these tests in GKE, we spawn a source and destination
   * postgres database ane use them for testing.
   */
  private PostgreSQLContainer sourcePsql;
  private PostgreSQLContainer destinationPsql;
  private GenericContainer echoServer;
  private AirbyteTestContainer airbyteTestContainer;
  private AirbyteApiClient apiClient;
  private final UUID defaultWorkspaceId;
  private final String postgresSqlInitFile;

  private KubernetesClient kubernetesClient;

  private List<UUID> sourceIds;
  private List<UUID> connectionIds;
  private List<UUID> destinationIds;
  private List<UUID> operationIds;
  private List<UUID> sourceDefinitionIds;
  private DataSource sourceDataSource;
  private DataSource destinationDataSource;
  private String postgresPassword;

  public PostgreSQLContainer getSourcePsql() {
    return sourcePsql;
  }

  public KubernetesClient getKubernetesClient() {
    return kubernetesClient;
  }

  public void removeConnection(final UUID connection) {
    connectionIds.remove(connection);
  }

  public void setApiClient(final AirbyteApiClient apiClient) {
    this.apiClient = apiClient;
  }

  public AirbyteAcceptanceTestHarness(final AirbyteApiClient apiClient, final UUID defaultWorkspaceId, final String postgresSqlInitFile)
      throws URISyntaxException, IOException, InterruptedException {
    // reads env vars to assign static variables
    assignEnvVars();
    this.apiClient = apiClient;
    this.defaultWorkspaceId = defaultWorkspaceId;
    this.postgresSqlInitFile = postgresSqlInitFile;

    if (isGke && !isKube) {
      throw new RuntimeException("KUBE Flag should also be enabled if GKE flag is enabled");
    }
    if (!isGke) {
      sourcePsql = new PostgreSQLContainer(SOURCE_POSTGRES_IMAGE_NAME)
          .withUsername(SOURCE_USERNAME)
          .withPassword(SOURCE_PASSWORD);
      sourcePsql.start();

      destinationPsql = new PostgreSQLContainer(DESTINATION_POSTGRES_IMAGE_NAME);
      destinationPsql.start();

      echoServer = new GenericContainer(DockerImageName.parse(ECHO_SERVER_IMAGE)).withExposedPorts(8080);
      echoServer.start();
    }

    if (isKube && !isGke) {
      // TODO(mfsiega-airbyte): get the Kube client to work with GKE tests. We don't use it yet but we
      // will want to someday.
      kubernetesClient = new DefaultKubernetesClient();
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

  public AirbyteAcceptanceTestHarness(final AirbyteApiClient apiClient, final UUID defaultWorkspaceId)
      throws URISyntaxException, IOException, InterruptedException {
    this(apiClient, defaultWorkspaceId, DEFAULT_POSTGRES_INIT_SQL_FILE);
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
      echoServer.stop();
    }

    if (airbyteTestContainer != null) {
      airbyteTestContainer.stop();
    }
  }

  public void setup() throws SQLException, URISyntaxException, IOException {
    sourceIds = Lists.newArrayList();
    connectionIds = Lists.newArrayList();
    destinationIds = Lists.newArrayList();
    operationIds = Lists.newArrayList();
    sourceDefinitionIds = Lists.newArrayList();

    if (isGke) {
      // Prepare the database data sources.
      LOGGER.info("postgresPassword: {}", postgresPassword);
      sourceDataSource = GKEPostgresConfig.getSourceDataSource(postgresPassword);
      destinationDataSource = GKEPostgresConfig.getDestinationDataSource(postgresPassword);
      // seed database.
      GKEPostgresConfig.runSqlScript(Path.of(MoreResources.readResourceAsFile(postgresSqlInitFile).toURI()), getSourceDatabase());
    } else {
      PostgreSQLContainerHelper.runSqlScript(MountableFile.forClasspathResource(postgresSqlInitFile), sourcePsql);

      destinationPsql = new PostgreSQLContainer("postgres:13-alpine");
      destinationPsql.start();

      sourceDataSource = DatabaseConnectionHelper.createDataSource(sourcePsql);
      destinationDataSource = DatabaseConnectionHelper.createDataSource(destinationPsql);
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
      } else {
        destinationPsql.stop();
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
    postgresPassword = System.getenv("POSTGRES_PASSWORD") != null
        ? System.getenv("POSTGRES_PASSWORD")
        : "admin123";
  }

  private WorkflowClient getWorkflowClient() {
    final TemporalUtils temporalUtils = new TemporalUtils(null, null, null, null, null, null, null);
    final WorkflowServiceStubs temporalService = temporalUtils.createTemporalService(
        TemporalWorkflowUtils.getAirbyteTemporalOptions("localhost:7233"),
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

  public AirbyteCatalog discoverSourceSchema(final UUID sourceId) throws ApiException {
    return discoverSourceSchemaWithId(sourceId).getCatalog();
  }

  public SourceDiscoverSchemaRead discoverSourceSchemaWithId(final UUID sourceId) throws ApiException {
    return AirbyteApiClient.retryWithJitter(
        () -> {
          final var result = apiClient.getSourceApi().discoverSchemaForSource(new SourceDiscoverSchemaRequestBody().sourceId(sourceId));
          if (result.getCatalog() == null) {
            throw new RuntimeException("no catalog returned, retrying...");
          }
          return result;
        },
        "discover source schema", 10, 60, 3);
  }

  // Run check Connection workflow.
  public void checkConnection(final UUID sourceId) throws ApiException {
    apiClient.getSourceApi().checkConnectionToSource(new SourceIdRequestBody().sourceId(sourceId));
  }

  public AirbyteCatalog discoverSourceSchemaWithoutCache(final UUID sourceId) throws ApiException {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getSourceApi().discoverSchemaForSource(
        new SourceDiscoverSchemaRequestBody().sourceId(sourceId).disableCache(true)).getCatalog(), "discover source schema no cache", 10, 60, 3);
  }

  public DestinationDefinitionSpecificationRead getDestinationDefinitionSpec(final UUID destinationDefinitionId, final UUID workspaceId)
      throws ApiException {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getDestinationDefinitionSpecificationApi()
        .getDestinationDefinitionSpecification(
            new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(destinationDefinitionId).workspaceId(workspaceId)),
        "get destination definition spec", 10, 60, 3);
  }

  public SourceDefinitionSpecificationRead getSourceDefinitionSpec(final UUID sourceDefinitionId) throws ApiException {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getSourceDefinitionSpecificationApi()
        .getSourceDefinitionSpecification(
            new SourceDefinitionIdWithWorkspaceId().sourceDefinitionId(sourceDefinitionId).workspaceId(UUID.randomUUID())),
        "get source definition spec", 10, 60, 3);
  }

  public void assertSourceAndDestinationDbInSync(final boolean withScdTable) throws Exception {
    // NOTE: this is an unusual use of retry. The intention is to tolerate eventual consistency if e.g.,
    // the sync is marked complete but the destination tables aren't finalized yet.
    AirbyteApiClient.retryWithJitterThrows(() -> {
      try {
        final Database source = getSourceDatabase();
        final Set<SchemaTableNamePair> sourceTables = listAllTables(source);
        final Set<SchemaTableNamePair> sourceTablesWithRawTablesAdded = addAirbyteGeneratedTables(withScdTable, sourceTables);
        final Database destination = getDestinationDatabase();
        final Set<SchemaTableNamePair> destinationTables = listAllTables(destination);
        assertEquals(sourceTablesWithRawTablesAdded, destinationTables,
            String.format("streams did not match.\n source stream names: %s\n destination stream names: %s\n", sourceTables, destinationTables));

        for (final SchemaTableNamePair pair : sourceTables) {
          final List<JsonNode> sourceRecords = retrieveRecordsFromDatabase(source, pair.getFullyQualifiedTableName());
          assertRawDestinationContains(sourceRecords, pair);
        }
      } catch (final Exception e) {
        return e;
      }
      return null;
    }, "assert source and destination in sync", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
  }

  public Database getSourceDatabase() {
    return getDatabase(sourceDataSource);
  }

  public Database getDestinationDatabase() {
    return getDatabase(destinationDataSource);
  }

  public Database getDatabase(final DataSource dataSource) {
    return new Database(DatabaseConnectionHelper.createDslContext(dataSource, SQLDialect.POSTGRES));
  }

  public Set<SchemaTableNamePair> listAllTables(final Database database) throws SQLException {
    return database.query(
        context -> {
          final Result<Record> fetch =
              context.fetch(
                  "SELECT tablename, schemaname FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'");
          return fetch.stream()
              .map(record -> {
                final var schemaName = (String) record.get("schemaname");
                final var tableName = (String) record.get("tablename");
                return new SchemaTableNamePair(schemaName, tableName);
              })
              .collect(Collectors.toSet());
        });
  }

  private Set<SchemaTableNamePair> addAirbyteGeneratedTables(final boolean withScdTable, final Set<SchemaTableNamePair> sourceTables) {
    return sourceTables.stream().flatMap(x -> {
      final String cleanedNameStream = x.tableName().replace(".", "_");
      final List<SchemaTableNamePair> explodedStreamNames = new ArrayList<>(List.of(
          new SchemaTableNamePair(OUTPUT_NAMESPACE_PREFIX + x.schemaName(),
              String.format("_airbyte_raw_%s%s", OUTPUT_STREAM_PREFIX, cleanedNameStream)),
          new SchemaTableNamePair(OUTPUT_NAMESPACE_PREFIX + x.schemaName(), String.format("%s%s", OUTPUT_STREAM_PREFIX, cleanedNameStream))));
      if (withScdTable) {
        explodedStreamNames
            .add(new SchemaTableNamePair("_airbyte_" + OUTPUT_NAMESPACE_PREFIX + x.schemaName(),
                String.format("%s%s_stg", OUTPUT_STREAM_PREFIX, cleanedNameStream)));
        explodedStreamNames
            .add(new SchemaTableNamePair(OUTPUT_NAMESPACE_PREFIX + x.schemaName(),
                String.format("%s%s_scd", OUTPUT_STREAM_PREFIX, cleanedNameStream)));
      }
      return explodedStreamNames.stream();
    }).collect(Collectors.toSet());
  }

  public void assertRawDestinationContains(final List<JsonNode> sourceRecords, final SchemaTableNamePair pair) throws Exception {
    final Set<JsonNode> destinationRecords = new HashSet<>(retrieveRawDestinationRecords(pair));

    assertEquals(sourceRecords.size(), destinationRecords.size(),
        String.format("destination contains: %s record. source contains: %s, \nsource records %s \ndestination records: %s",
            destinationRecords.size(), sourceRecords.size(), sourceRecords, destinationRecords));

    for (final JsonNode sourceStreamRecord : sourceRecords) {
      assertTrue(destinationRecords.contains(sourceStreamRecord),
          String.format("destination does not contain record:\n %s \n destination contains:\n %s\n",
              sourceStreamRecord, destinationRecords));
    }
  }

  public void assertNormalizedDestinationContains(final List<JsonNode> sourceRecords) throws Exception {
    assertNormalizedDestinationContains(sourceRecords, STREAM_NAME);
  }

  public void assertNormalizedDestinationContains(final List<JsonNode> sourceRecords, final String streamName) throws Exception {
    final Database destination = getDestinationDatabase();
    final String finalDestinationTable = String.format("%spublic.%s%s", OUTPUT_NAMESPACE_PREFIX, OUTPUT_STREAM_PREFIX, streamName.replace(".", "_"));
    final List<JsonNode> destinationRecords = retrieveRecordsFromDatabase(destination, finalDestinationTable);

    assertEquals(sourceRecords.size(), destinationRecords.size(),
        String.format("destination contains: %s record. source contains: %s", sourceRecords.size(), destinationRecords.size()));

    for (final JsonNode sourceStreamRecord : sourceRecords) {
      assertTrue(
          destinationRecords.stream()
              .anyMatch(r -> r.get(COLUMN_NAME).asText().equals(sourceStreamRecord.get(COLUMN_NAME).asText())
                  && r.get(COLUMN_ID).asInt() == sourceStreamRecord.get(COLUMN_ID).asInt()),
          String.format("destination does not contain record:\n %s \n destination contains:\n %s\n", sourceStreamRecord, destinationRecords));
    }
  }

  /**
   * Assert that the normalized destination matches the input records, only expecting a single id
   * column.
   *
   * @param expectedRecords the records that we expect
   * @throws Exception while retrieving sources
   */
  public void assertNormalizedDestinationContainsIdColumn(final List<JsonNode> expectedRecords) throws Exception {
    final Database destination = getDestinationDatabase();
    final String finalDestinationTable = String.format("%spublic.%s%s", OUTPUT_NAMESPACE_PREFIX, OUTPUT_STREAM_PREFIX, STREAM_NAME.replace(".", "_"));
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

  @Deprecated() // prefer to include the catalog id below
  public ConnectionRead createConnection(final String name,
                                         final UUID sourceId,
                                         final UUID destinationId,
                                         final List<UUID> operationIds,
                                         final AirbyteCatalog catalog,
                                         final ConnectionScheduleType scheduleType,
                                         final ConnectionScheduleData scheduleData)
      throws Exception {
    return createConnectionWithGeography(name, sourceId, destinationId, operationIds, catalog, scheduleType, scheduleData, Geography.AUTO);
  }

  public ConnectionRead createConnection(String name,
                                         UUID sourceId,
                                         UUID destinationId,
                                         List<UUID> operationIds,
                                         AirbyteCatalog catalog,
                                         UUID catalogId,
                                         ConnectionScheduleType scheduleType,
                                         ConnectionScheduleData scheduleData)
      throws Exception {
    return createConnectionFromRequest(
        new ConnectionCreate()
            .status(ConnectionStatus.ACTIVE)
            .sourceId(sourceId)
            .destinationId(destinationId)
            .syncCatalog(catalog)
            .sourceCatalogId(catalogId)
            .scheduleType(scheduleType)
            .scheduleData(scheduleData)
            .operationIds(operationIds)
            .name(name)
            .namespaceDefinition(NamespaceDefinitionType.CUSTOMFORMAT)
            .namespaceFormat(OUTPUT_NAMESPACE)
            .prefix(OUTPUT_STREAM_PREFIX)
            .geography(Geography.AUTO));
  }

  public ConnectionRead createConnectionWithGeography(final String name,
                                                      final UUID sourceId,
                                                      final UUID destinationId,
                                                      final List<UUID> operationIds,
                                                      final AirbyteCatalog catalog,
                                                      final ConnectionScheduleType scheduleType,
                                                      final ConnectionScheduleData scheduleData,
                                                      final Geography geography)
      throws Exception {
    return createConnectionFromRequest(
        new ConnectionCreate()
            .status(ConnectionStatus.ACTIVE)
            .sourceId(sourceId)
            .destinationId(destinationId)
            .syncCatalog(catalog)
            .scheduleType(scheduleType)
            .scheduleData(scheduleData)
            .operationIds(operationIds)
            .name(name)
            .namespaceDefinition(NamespaceDefinitionType.CUSTOMFORMAT)
            .namespaceFormat(OUTPUT_NAMESPACE)
            .prefix(OUTPUT_STREAM_PREFIX)
            .geography(geography));
  }

  private ConnectionRead createConnectionFromRequest(final ConnectionCreate request) throws Exception {
    final ConnectionRead connection = AirbyteApiClient.retryWithJitterThrows(() -> apiClient.getConnectionApi().createConnection(request),
        "create connection", 10, 60, 3);
    connectionIds.add(connection.getConnectionId());
    return connection;
  }

  public ConnectionRead getConnection(final UUID connectionId) throws Exception {
    return AirbyteApiClient.retryWithJitterThrows(
        () -> apiClient.getConnectionApi().getConnection(new ConnectionIdRequestBody().connectionId(connectionId)), "get connection",
        10, 60, 3);
  }

  public void updateConnectionSchedule(
                                       final UUID connectionId,
                                       final ConnectionScheduleType newScheduleType,
                                       final ConnectionScheduleData newScheduleData)
      throws ApiException {
    AirbyteApiClient.retryWithJitter(() -> apiClient.getConnectionApi().updateConnection(
        new ConnectionUpdate()
            .connectionId(connectionId)
            .scheduleType(newScheduleType)
            .scheduleData(newScheduleData)),
        "update connection", 10, 60, 3);
  }

  public void updateConnectionCatalog(final UUID connectionId, final AirbyteCatalog catalog) throws ApiException {
    AirbyteApiClient.retryWithJitter(() -> apiClient.getConnectionApi().updateConnection(
        new ConnectionUpdate()
            .connectionId(connectionId)
            .syncCatalog(catalog)),
        "update connection catalog", 10, 60, 3);
  }

  public JobInfoRead syncConnection(final UUID connectionId) {
    return AirbyteApiClient.retryWithJitter(
        () -> apiClient.getConnectionApi().syncConnection(new ConnectionIdRequestBody().connectionId(connectionId)),
        "sync connection", 10, 60, 3);
  }

  public JobInfoRead cancelSync(final long jobId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getJobsApi().cancelJob(new JobIdRequestBody().id(jobId)),
        "cancel sync job", 10, 60, 3);
  }

  public JobInfoRead resetConnection(final UUID connectionId) {
    return AirbyteApiClient.retryWithJitter(
        () -> apiClient.getConnectionApi().resetConnection(new ConnectionIdRequestBody().connectionId(connectionId)),
        "reset connection", 10, 60, 3);
  }

  public void deleteConnection(final UUID connectionId) {
    AirbyteApiClient.retryWithJitter(() -> {
      apiClient.getConnectionApi().deleteConnection(new ConnectionIdRequestBody().connectionId(connectionId));
      return null;
    }, "delete connection", 10, 60, 3);
  }

  public DestinationRead createPostgresDestination(final boolean isLegacy) throws ApiException {
    return createDestination(
        "AccTestDestination-" + UUID.randomUUID(),
        defaultWorkspaceId,
        getPostgresDestinationDefinitionId(),
        getDestinationDbConfig(isLegacy));
  }

  public DestinationRead createPostgresDestination() throws ApiException {
    return createPostgresDestination(false);
  }

  public DestinationRead createDestination(final String name,
                                           final UUID workspaceId,
                                           final UUID destinationDefId,
                                           final JsonNode destinationConfig)
      throws ApiException {
    final DestinationRead destination =
        AirbyteApiClient.retryWithJitter(() -> apiClient.getDestinationApi().createDestination(new DestinationCreate()
            .name(name)
            .connectionConfiguration(Jsons.jsonNode(destinationConfig))
            .workspaceId(workspaceId)
            .destinationDefinitionId(destinationDefId)), "create destination", 10, 60, 3);
    destinationIds.add(destination.getDestinationId());
    return destination;
  }

  public CheckConnectionRead.StatusEnum checkDestination(final UUID destinationId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getDestinationApi()
        .checkConnectionToDestination(new DestinationIdRequestBody().destinationId(destinationId))
        .getStatus(), "check connection", 10, 60, 3);
  }

  public OperationRead createOperation() throws ApiException {
    return createOperation(defaultWorkspaceId);
  }

  public OperationRead createOperation(final UUID workspaceId) throws ApiException {
    final OperatorConfiguration normalizationConfig = new OperatorConfiguration()
        .operatorType(OperatorType.NORMALIZATION).normalization(new OperatorNormalization().option(
            OperatorNormalization.OptionEnum.BASIC));

    final OperationCreate operationCreate = new OperationCreate()
        .workspaceId(workspaceId)
        .name("AccTestDestination-" + UUID.randomUUID()).operatorConfiguration(normalizationConfig);

    final OperationRead operation = AirbyteApiClient.retryWithJitter(() -> apiClient.getOperationApi().createOperation(operationCreate),
        "create operation", 10, 60, 3);
    operationIds.add(operation.getOperationId());
    return operation;
  }

  public List<JsonNode> retrieveRecordsFromDatabase(final Database database, final String table) throws SQLException {
    return database.query(context -> context.fetch(String.format("SELECT * FROM %s;", table)))
        .stream()
        .map(Record::intoMap)
        .map(Jsons::jsonNode)
        .collect(Collectors.toList());
  }

  public List<JsonNode> retrieveDestinationRecords(final Database database, final String table) throws SQLException {
    return database.query(context -> context.fetch(String.format("SELECT * FROM %s;", table)))
        .stream()
        .map(Record::intoMap)
        .map(r -> r.get(COLUMN_NAME_DATA))
        .map(f -> (JSONB) f)
        .map(JSONB::data)
        .map(Jsons::deserialize)
        .map(Jsons::jsonNode)
        .collect(Collectors.toList());
  }

  public List<JsonNode> retrieveRawDestinationRecords(final SchemaTableNamePair pair) throws Exception {
    final Database destination = getDestinationDatabase();
    final Set<SchemaTableNamePair> namePairs = listAllTables(destination);

    final String rawStreamName = String.format("_airbyte_raw_%s%s", OUTPUT_STREAM_PREFIX, pair.tableName().replace(".", "_"));
    final SchemaTableNamePair rawTablePair = new SchemaTableNamePair(OUTPUT_NAMESPACE_PREFIX + pair.schemaName(), rawStreamName);
    assertTrue(namePairs.contains(rawTablePair), "can't find a non-normalized version (raw) of " + rawTablePair.getFullyQualifiedTableName());

    return retrieveDestinationRecords(destination, rawTablePair.getFullyQualifiedTableName());
  }

  public JsonNode getSourceDbConfig(final boolean isLegacy) {
    return getDbConfig(sourcePsql, false, false, isLegacy, Type.SOURCE);
  }

  public JsonNode getSourceDbConfig() {
    return getSourceDbConfig(false);
  }

  public JsonNode getDestinationDbConfig(final boolean isLegacy) {
    return getDbConfig(destinationPsql, false, true, isLegacy, Type.DESTINATION);
  }

  public JsonNode getDestinationDbConfig() {
    return getDestinationDbConfig(false);
  }

  public JsonNode getDestinationDbConfigWithHiddenPassword() {
    return getDbConfig(destinationPsql, true, true, false, Type.DESTINATION);
  }

  public JsonNode getDbConfig(final PostgreSQLContainer psql,
                              final boolean hiddenPassword,
                              final boolean withSchema,
                              final boolean isLegacy,
                              final Type connectorType) {
    try {
      final Map<Object, Object> dbConfig =
          (isKube && isGke) ? GKEPostgresConfig.dbConfig(connectorType, hiddenPassword ? null : postgresPassword, withSchema)
              : localConfig(psql, hiddenPassword, withSchema, isLegacy);
      final var config = Jsons.jsonNode(dbConfig);
      LOGGER.info("Using db config: {}", Jsons.toPrettyString(config));
      return config;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map<Object, Object> localConfig(final PostgreSQLContainer psql,
                                          final boolean hiddenPassword,
                                          final boolean withSchema,
                                          final boolean isLegacy)
      throws UnknownHostException {
    final Map<Object, Object> dbConfig = new HashMap<>();
    // don't use psql.getHost() directly since the ip we need differs depending on environment
    dbConfig.put(JdbcUtils.HOST_KEY, getHostname());

    if (hiddenPassword) {
      dbConfig.put(JdbcUtils.PASSWORD_KEY, "**********");
    } else {
      dbConfig.put(JdbcUtils.PASSWORD_KEY, psql.getPassword());
    }

    dbConfig.put(JdbcUtils.PORT_KEY, psql.getFirstMappedPort());
    dbConfig.put(JdbcUtils.DATABASE_KEY, psql.getDatabaseName());
    dbConfig.put(JdbcUtils.USERNAME_KEY, psql.getUsername());

    // bypasses the SSL modification for cloud acceptance tests. This use useful in cloud since it
    // enforces most databases to have SSL on, but the postgres containers we use for testing does not
    // allow SSL.
    if (!isLegacy) {
      dbConfig.put(IS_TEST, true);
    }
    dbConfig.put(JdbcUtils.SSL_KEY, false);

    if (withSchema) {
      dbConfig.put(JdbcUtils.SCHEMA_KEY, "public");
    }
    return dbConfig;
  }

  public String getEchoServerUrl() {
    if (isKube && isGke) {
      return "http://acceptance-test-echo-server-source-svc.acceptance-tests.svc.cluster.local:8080";
    } else if (isGke) {
      return "http://localhost:6000";
    }
    return String.format("http://%s:%s/", getHostname(), echoServer.getFirstMappedPort());
  }

  private String getHostname() {
    if (isKube) {
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

  public JobInfoRead getJobInfoRead(long id) {
    try {
      return apiClient.getJobsApi().getJobInfo(new JobIdRequestBody().id(id));
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  public SourceDefinitionRead createE2eSourceDefinition(final UUID workspaceId) throws ApiException {
    final var sourceDefinitionRead = AirbyteApiClient.retryWithJitter(
        () -> apiClient.getSourceDefinitionApi().createCustomSourceDefinition(new CustomSourceDefinitionCreate()
            .workspaceId(workspaceId)
            .sourceDefinition(new SourceDefinitionCreate()
                .name("E2E Test Source")
                .dockerRepository("airbyte/source-e2e-test")
                .dockerImageTag(SOURCE_E2E_TEST_CONNECTOR_VERSION)
                .documentationUrl(URI.create("https://example.com")))),
        "create customer source definition", 10, 60, 3);
    sourceDefinitionIds.add(sourceDefinitionRead.getSourceDefinitionId());
    return sourceDefinitionRead;
  }

  public DestinationDefinitionRead createE2eDestinationDefinition(final UUID workspaceId) throws ApiException {
    return apiClient.getDestinationDefinitionApi().createCustomDestinationDefinition(new CustomDestinationDefinitionCreate()
        .workspaceId(workspaceId)
        .destinationDefinition(new DestinationDefinitionCreate()
            .name("E2E Test Destination")
            .dockerRepository("airbyte/destination-e2e-test")
            .dockerImageTag(DESTINATION_E2E_TEST_CONNECTOR_VERSION)
            .documentationUrl(URI.create("https://example.com"))));
  }

  public SourceRead createPostgresSource(final boolean isLegacy) throws ApiException {
    return createSource(
        "acceptanceTestDb-" + UUID.randomUUID(),
        defaultWorkspaceId,
        getPostgresSourceDefinitionId(),
        getSourceDbConfig(isLegacy));
  }

  public SourceRead createPostgresSource() throws ApiException {
    return createPostgresSource(false);
  }

  public SourceRead createSource(final String name, final UUID workspaceId, final UUID sourceDefId, final JsonNode sourceConfig)
      throws ApiException {
    final SourceRead source = AirbyteApiClient.retryWithJitter(() -> apiClient.getSourceApi().createSource(new SourceCreate()
        .name(name)
        .sourceDefinitionId(sourceDefId)
        .workspaceId(workspaceId)
        .connectionConfiguration(sourceConfig)), "create source", 10, 60, 3);
    sourceIds.add(source.getSourceId());
    return source;
  }

  public CheckConnectionRead checkSource(final UUID sourceId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getSourceApi().checkConnectionToSource(new SourceIdRequestBody().sourceId(sourceId)),
        "check source", 10, 60, 3);
  }

  public UUID getPostgresSourceDefinitionId() throws ApiException {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getSourceDefinitionApi().listSourceDefinitions().getSourceDefinitions()
        .stream()
        .filter(sourceRead -> "postgres".equalsIgnoreCase(sourceRead.getName()))
        .findFirst()
        .orElseThrow()
        .getSourceDefinitionId(), "get postgres definition", 10, 60, 3);
  }

  public UUID getPostgresDestinationDefinitionId() throws ApiException {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getDestinationDefinitionApi().listDestinationDefinitions().getDestinationDefinitions()
        .stream()
        .filter(destRead -> "postgres".equalsIgnoreCase(destRead.getName()))
        .findFirst()
        .orElseThrow()
        .getDestinationDefinitionId(), "get postgres definition", 10, 60, 3);
  }

  public void updateSourceDefinitionVersion(final UUID sourceDefinitionId, final String dockerImageTag) throws ApiException {
    apiClient.getSourceDefinitionApi().updateSourceDefinition(new SourceDefinitionUpdate()
        .sourceDefinitionId(sourceDefinitionId).dockerImageTag(dockerImageTag));
  }

  public void updateDestinationDefinitionVersion(final UUID destDefinitionId, final String dockerImageTag) throws ApiException {
    apiClient.getDestinationDefinitionApi().updateDestinationDefinition(new DestinationDefinitionUpdate()
        .destinationDefinitionId(destDefinitionId).dockerImageTag(dockerImageTag));
  }

  private void clearSourceDbData() throws SQLException {
    final Database database = getSourceDatabase();
    final Set<SchemaTableNamePair> pairs = listAllTables(database);
    for (final SchemaTableNamePair pair : pairs) {
      LOGGER.debug("Clearing table {} {}", pair.schemaName(), pair.tableName());
      database.query(context -> context.execute(String.format("DROP TABLE %s.%s", pair.schemaName(), pair.tableName())));
    }
  }

  private void clearDestinationDbData() throws SQLException {
    final Database database = getDestinationDatabase();
    final Set<SchemaTableNamePair> pairs = listAllTables(database);
    for (final SchemaTableNamePair pair : pairs) {
      LOGGER.debug("Clearing table {} {}", pair.schemaName(), pair.tableName());
      database.query(context -> context.execute(String.format("DROP TABLE %s.%s CASCADE", pair.schemaName(), pair.tableName())));
    }
  }

  private void disableConnection(final UUID connectionId) throws ApiException {
    final ConnectionUpdate connectionUpdate =
        new ConnectionUpdate().connectionId(connectionId).status(ConnectionStatus.DEPRECATED);
    apiClient.getConnectionApi().updateConnection(connectionUpdate);
  }

  private void deleteSource(final UUID sourceId) throws ApiException {
    AirbyteApiClient.retryWithJitter(() -> {
      apiClient.getSourceApi().deleteSource(new SourceIdRequestBody().sourceId(sourceId));
      return null; // Note: the retryWithJitter needs a return.
    }, "delete source", 10, 60, 3);
  }

  private void deleteDestination(final UUID destinationId) throws ApiException {
    AirbyteApiClient.retryWithJitter(() -> {
      apiClient.getDestinationApi().deleteDestination(new DestinationIdRequestBody().destinationId(destinationId));
      return null; // Note: the retryWithJitter needs a return.
    }, "delete destination", 10, 60, 3);
  }

  private void deleteOperation(final UUID destinationId) throws ApiException {
    AirbyteApiClient.retryWithJitter(() -> {
      apiClient.getOperationApi().deleteOperation(new OperationIdRequestBody().operationId(destinationId));
      return null;
    }, "delete operation", 10, 60, 3);
  }

  @Deprecated(forRemoval = true)
  public JobRead getMostRecentSyncJobId(final UUID connectionId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getJobsApi()
        .listJobsFor(new JobListRequestBody().configId(connectionId.toString()).configTypes(List.of(JobConfigType.SYNC)))
        .getJobs()
        .stream().findFirst().map(JobWithAttemptsRead::getJob).orElseThrow(), "get most recent sync job", 10, 60, 3);
  }

  /**
   * Returns the most recent job for the provided connection.
   */
  public JobRead getMostRecentSyncForConnection(final UUID connectionId) {
    return AirbyteApiClient.retryWithJitter(() -> apiClient.getJobsApi()
        .listJobsFor(new JobListRequestBody().configId(connectionId.toString()).configTypes(List.of(JobConfigType.SYNC)))
        .getJobs()
        .stream().findFirst().map(JobWithAttemptsRead::getJob).orElseThrow(), "get most recent sync job", 10, 60, 3);
  }

  public static void waitForSuccessfulJob(final JobsApi jobsApi, final JobRead originalJob) throws InterruptedException, ApiException {
    final JobRead job = waitWhileJobHasStatus(jobsApi, originalJob, Sets.newHashSet(JobStatus.PENDING, JobStatus.RUNNING, JobStatus.INCOMPLETE));

    if (!JobStatus.SUCCEEDED.equals(job.getStatus())) {
      // If a job failed during testing, show us why.
      final JobIdRequestBody id = new JobIdRequestBody();
      id.setId(originalJob.getId());
      for (final AttemptInfoRead attemptInfo : jobsApi.getJobInfo(id).getAttempts()) {
        LOGGER.warn("Unsuccessful job attempt " + attemptInfo.getAttempt().getId()
            + " with status " + job.getStatus() + " produced log output as follows: " + attemptInfo.getLogs().getLogLines());
      }
    }
    assertEquals(JobStatus.SUCCEEDED, job.getStatus());
    Thread.sleep(200);
  }

  public static JobRead waitWhileJobHasStatus(final JobsApi jobsApi, final JobRead originalJob, final Set<JobStatus> jobStatuses)
      throws InterruptedException {
    return waitWhileJobHasStatus(jobsApi, originalJob, jobStatuses, Duration.ofMinutes(6));
  }

  @SuppressWarnings("BusyWait")
  public static JobRead waitWhileJobHasStatus(final JobsApi jobsApi,
                                              final JobRead originalJob,
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
        job = jobsApi.getJobInfo(new JobIdRequestBody().id(job.getId())).getJob();
      } catch (final ApiException e) {
        // TODO(mfsiega-airbyte): consolidate our polling/retrying logic.
        LOGGER.warn("error querying jobs api, retrying...");
      }
      LOGGER.info("waiting: job id: {} config type: {} status: {}", job.getId(), job.getConfigType(), job.getStatus());
    }
    return job;
  }

  @SuppressWarnings("BusyWait")
  public static void waitWhileJobIsRunning(final JobsApi jobsApi, final JobRead job, final Duration maxWaitTime)
      throws ApiException, InterruptedException {
    final Instant waitStart = Instant.now();
    JobDebugInfoRead jobDebugInfoRead = jobsApi.getJobDebugInfo(new JobIdRequestBody().id(job.getId()));
    LOGGER.info("workflow state: {}", jobDebugInfoRead.getWorkflowState());
    while (jobDebugInfoRead.getWorkflowState() != null && jobDebugInfoRead.getWorkflowState().getRunning()) {
      if (Duration.between(waitStart, Instant.now()).compareTo(maxWaitTime) > 0) {
        LOGGER.info("Max wait time of {} has been reached. Stopping wait.", maxWaitTime);
        break;
      }
      LOGGER.info("waiting: job id: {}, workflowState.isRunning is still true", job.getId());
      sleep(1000);
      jobDebugInfoRead = jobsApi.getJobDebugInfo(new JobIdRequestBody().id(job.getId()));
    }
  }

  @SuppressWarnings("BusyWait")
  public static ConnectionState waitForConnectionState(final AirbyteApiClient apiClient, final UUID connectionId)
      throws ApiException, InterruptedException {
    ConnectionState connectionState = AirbyteApiClient.retryWithJitter(
        () -> apiClient.getStateApi().getState(new ConnectionIdRequestBody().connectionId(connectionId)), "get connection state", 10, 60, 3);
    int count = 0;
    while (count < 60 && (connectionState.getState() == null || connectionState.getState().isNull())) {
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
  public void waitForSuccessfulSyncNoTimeout(final JobRead jobRead) throws InterruptedException {
    var job = jobRead;
    while (IN_PROGRESS_JOB_STATUSES.contains(job.getStatus())) {
      job = getJobInfoRead(job.getId()).getJob();
      LOGGER.info("waiting: job id: {} config type: {} status: {}", job.getId(), job.getConfigType(), job.getStatus());
      sleep(3000);
    }
    assertEquals(JobStatus.SUCCEEDED, job.getStatus());
  }

  public JobRead waitUntilTheNextJobIsStarted(final UUID connectionId) throws Exception {
    final JobRead lastJob = getMostRecentSyncJobId(connectionId);
    if (lastJob.getStatus() != JobStatus.SUCCEEDED) {
      return lastJob;
    }

    JobRead mostRecentSyncJob = getMostRecentSyncJobId(connectionId);
    int count = 0;
    while (count < MAX_ALLOWED_SECOND_PER_RUN && mostRecentSyncJob.getId().equals(lastJob.getId())) {
      Thread.sleep(Duration.ofSeconds(1).toMillis());
      mostRecentSyncJob = getMostRecentSyncJobId(connectionId);
      ++count;
    }
    final boolean exceeded120seconds = count >= MAX_ALLOWED_SECOND_PER_RUN;
    if (exceeded120seconds) {
      // Fail because taking more than 60seconds to start a job is not expected
      // Returning the current mostRecentSyncJob here could end up hiding some issues
      Assertions.fail("unable to find the next job within 60seconds");
    }
    LOGGER.info("Time to run the job: " + count);
    return mostRecentSyncJob;
  }

  /**
   * Connector type.
   */
  public enum Type {
    SOURCE,
    DESTINATION
  }

  public void assertDestinationDbEmpty(final boolean withScdTable) throws Exception {
    final Database source = getSourceDatabase();
    final Set<SchemaTableNamePair> sourceTables = listAllTables(source);
    final Set<SchemaTableNamePair> sourceTablesWithRawTablesAdded = addAirbyteGeneratedTables(withScdTable, sourceTables);
    final Database destination = getDestinationDatabase();
    final Set<SchemaTableNamePair> destinationTables = listAllTables(destination);
    assertEquals(sourceTablesWithRawTablesAdded, destinationTables,
        String.format("streams did not match.\n source stream names: %s\n destination stream names: %s\n", sourceTables, destinationTables));

    for (final SchemaTableNamePair pair : sourceTables) {
      final List<JsonNode> sourceRecords = retrieveRawDestinationRecords(pair);
      assertTrue(sourceRecords.isEmpty());
    }
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

  /**
   * Asserts that all streams associated with the most recent job and attempt for the provided
   * connection are the in expected state.
   *
   * @param workspaceId The workspace that contains the connection.
   * @param connectionId The connection that just executed a sync or reset.
   * @param jobInfoRead The job associated with the sync execution.
   * @param expectedRunState The expected stream status for each stream in the connection for the most
   *        recent job and attempt.
   * @param expectedJobType The expected type of the stream status.
   */
  public void assertStreamStatuses(final UUID workspaceId,
                                   final UUID connectionId,
                                   final JobInfoRead jobInfoRead,
                                   final StreamStatusRunState expectedRunState,
                                   final StreamStatusJobType expectedJobType) {
    final var jobId = jobInfoRead.getJob().getId();
    final var attemptNumber = jobInfoRead.getAttempts().size() - 1;

    final List<StreamStatusRead> streamStatuses = fetchStreamStatus(workspaceId, connectionId, jobId, attemptNumber);
    assertNotNull(streamStatuses);
    final List<StreamStatusRead> filteredStreamStatuses = streamStatuses.stream().filter(s -> expectedJobType.equals(s.getJobType())).toList();
    assertFalse(filteredStreamStatuses.isEmpty());
    filteredStreamStatuses.forEach(status -> assertEquals(expectedRunState, status.getRunState()));

  }

  /**
   * Fetches the stream status associated with the provided information, retrying for a set amount of
   * attempts if the status is currently not available.
   *
   * @param workspaceId The workspace that contains the connection.
   * @param connectionId The connection that has been executed by a test
   * @param jobId The job ID associated with the sync execution.
   * @param attempt The attempt number associated with the sync execution.
   */
  private List<StreamStatusRead> fetchStreamStatus(final UUID workspaceId,
                                                   final UUID connectionId,
                                                   final Long jobId,
                                                   final Integer attempt) {
    List<StreamStatusRead> results = List.of();
    final StreamStatusListRequestBody streamStatusListRequestBody = new StreamStatusListRequestBody()
        .connectionId(connectionId)
        .jobId(jobId)
        .attemptNumber(attempt)
        .workspaceId(workspaceId)
        .pagination(new Pagination().pageSize(100).rowOffset(0));

    int count = 0;
    while (count < 60 && results.isEmpty()) {
      LOGGER.debug("Fetching stream status for {}...", streamStatusListRequestBody);
      try {
        final StreamStatusReadList result = apiClient.getStreamStatusesApi().getStreamStatuses(streamStatusListRequestBody);
        if (result != null) {
          LOGGER.debug("Stream status result for connection {}: {}", connectionId, result);
          results = result.getStreamStatuses();
        }
      } catch (final ApiException e) {
        LOGGER.info("Unable to call stream status API.", e);
      }
      count++;

      if (results.isEmpty()) {
        try {
          sleep(5000);
        } catch (final InterruptedException e) {
          LOGGER.debug("Failed to sleep.", e);
        }
      }
    }

    return results;
  }

}
