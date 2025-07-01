/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import dev.failsafe.Failsafe
import dev.failsafe.FailsafeException
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedRunnable
import dev.failsafe.function.CheckedSupplier
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ActorDefinitionRequestBody
import io.airbyte.api.client.model.generated.ActorType
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStream
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.AttemptInfoRead
import io.airbyte.api.client.model.generated.CheckConnectionRead
import io.airbyte.api.client.model.generated.ConnectionCreate
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionReadList
import io.airbyte.api.client.model.generated.ConnectionScheduleData
import io.airbyte.api.client.model.generated.ConnectionScheduleType
import io.airbyte.api.client.model.generated.ConnectionState
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.ConnectionUpdate
import io.airbyte.api.client.model.generated.CustomDestinationDefinitionCreate
import io.airbyte.api.client.model.generated.CustomSourceDefinitionCreate
import io.airbyte.api.client.model.generated.DataplaneGroupListRequestBody
import io.airbyte.api.client.model.generated.DestinationCreate
import io.airbyte.api.client.model.generated.DestinationDefinitionCreate
import io.airbyte.api.client.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.client.model.generated.DestinationDefinitionRead
import io.airbyte.api.client.model.generated.DestinationDefinitionSpecificationRead
import io.airbyte.api.client.model.generated.DestinationDefinitionUpdate
import io.airbyte.api.client.model.generated.DestinationIdRequestBody
import io.airbyte.api.client.model.generated.DestinationRead
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.DestinationUpdate
import io.airbyte.api.client.model.generated.GetAttemptStatsRequestBody
import io.airbyte.api.client.model.generated.JobConfigType
import io.airbyte.api.client.model.generated.JobIdRequestBody
import io.airbyte.api.client.model.generated.JobInfoRead
import io.airbyte.api.client.model.generated.JobListForWorkspacesRequestBody
import io.airbyte.api.client.model.generated.JobListRequestBody
import io.airbyte.api.client.model.generated.JobRead
import io.airbyte.api.client.model.generated.JobStatus
import io.airbyte.api.client.model.generated.JobWithAttemptsRead
import io.airbyte.api.client.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.client.model.generated.LogEvent
import io.airbyte.api.client.model.generated.LogFormatType
import io.airbyte.api.client.model.generated.NamespaceDefinitionType
import io.airbyte.api.client.model.generated.NonBreakingChangesPreference
import io.airbyte.api.client.model.generated.OperationCreate
import io.airbyte.api.client.model.generated.OperationIdRequestBody
import io.airbyte.api.client.model.generated.OperationRead
import io.airbyte.api.client.model.generated.OperatorConfiguration
import io.airbyte.api.client.model.generated.OperatorType
import io.airbyte.api.client.model.generated.OperatorWebhook
import io.airbyte.api.client.model.generated.OperatorWebhookDbtCloud
import io.airbyte.api.client.model.generated.Pagination
import io.airbyte.api.client.model.generated.SchemaChangeBackfillPreference
import io.airbyte.api.client.model.generated.SourceCreate
import io.airbyte.api.client.model.generated.SourceDefinitionCreate
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.client.model.generated.SourceDefinitionRead
import io.airbyte.api.client.model.generated.SourceDefinitionSpecificationRead
import io.airbyte.api.client.model.generated.SourceDefinitionUpdate
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRequestBody
import io.airbyte.api.client.model.generated.SourceIdRequestBody
import io.airbyte.api.client.model.generated.SourceRead
import io.airbyte.api.client.model.generated.SourceReadList
import io.airbyte.api.client.model.generated.SourceUpdate
import io.airbyte.api.client.model.generated.StreamStatusListRequestBody
import io.airbyte.api.client.model.generated.StreamStatusReadList
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.api.client.model.generated.WebBackendConnectionRead
import io.airbyte.api.client.model.generated.WebBackendConnectionRequestBody
import io.airbyte.api.client.model.generated.WebBackendConnectionUpdate
import io.airbyte.api.client.model.generated.WebBackendOperationCreateOrUpdate
import io.airbyte.api.client.model.generated.WorkspaceCreateWithId
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.MoreResources
import io.airbyte.commons.string.Strings
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.commons.temporal.TemporalWorkflowUtils
import io.airbyte.commons.temporal.config.TemporalSdkTimeouts
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow
import io.airbyte.commons.temporal.scheduling.state.WorkflowState
import io.airbyte.db.Database
import io.airbyte.db.factory.DataSourceFactory.close
import io.airbyte.db.jdbc.JdbcUtils
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.Flag
import io.airbyte.featureflag.tests.TestFlagsSetter
import io.airbyte.featureflag.tests.TestFlagsSetter.FlagOverride
import io.airbyte.test.utils.AcceptanceTestHarness
import io.airbyte.test.utils.Databases.createDataSource
import io.airbyte.test.utils.Databases.createDslContext
import io.airbyte.test.utils.Databases.listAllTables
import io.airbyte.test.utils.GKEPostgresConfig.dbConfig
import io.airbyte.test.utils.GKEPostgresConfig.getDataSource
import io.airbyte.test.utils.GKEPostgresConfig.runSqlScript
import io.airbyte.test.utils.PostgreSQLContainerHelper.runSqlScript
import io.temporal.client.WorkflowClient
import junit.framework.AssertionFailedError
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.SQLDialect
import org.junit.jupiter.api.Assertions
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile
import java.io.IOException
import java.net.Inet4Address
import java.net.URI
import java.net.URISyntaxException
import java.net.UnknownHostException
import java.nio.file.Path
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.util.Locale
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.stream.Collectors
import javax.sql.DataSource

/**
 * This class contains containers used for acceptance tests. Some of those containers/states are
 * only used when the test are run without GKE. Specific environmental variables govern what types
 * of containers are run.
 *
 *
 * This class is put in a separate module to be easily pulled in as a dependency for Airbyte Cloud
 * Acceptance Tests.
 *
 *
 * Containers and states include:
 *  * source postgres SQL
 *  * destination postgres SQL
 *  * kubernetes client
 *  * lists of UUIDS representing IDs of sources, destinations, connections, and operations
 */
class AcceptanceTestHarness
  @JvmOverloads
  constructor(
    @JvmField val apiClient: AirbyteApiClient,
    private val defaultWorkspaceId: UUID,
    private val postgresSqlInitFile: String? = DEFAULT_POSTGRES_INIT_SQL_FILE,
    val testFlagsSetter: TestFlagsSetter? = null,
  ) {
    @JvmField
    val dataplaneGroupId: UUID

    private val expectedAirbyteCatalog: AirbyteCatalog
    private val retryPolicy: RetryPolicy<Any>

    /**
     * When the acceptance tests are run against a local instance of docker-compose or KUBE then these
     * test containers are used. When we run these tests in GKE, we spawn a source and destination
     * postgres database ane use them for testing.
     */
    private var cloudSqlDatabaseProvisioner: CloudSqlDatabaseProvisioner? = null
    private var sourcePsql: PostgreSQLContainer<*>? = null
    private var destinationPsql: PostgreSQLContainer<*>? = null
    private var sourceDatabaseName: String? = null
    private var destinationDatabaseName: String? = null

    private val sourceIds: MutableList<UUID> = Lists.newArrayList()
    private val connectionIds: MutableList<UUID> = Lists.newArrayList()
    private val destinationIds: MutableList<UUID> = Lists.newArrayList()
    private val operationIds: List<UUID> = Lists.newArrayList()
    private val sourceDefinitionIds: MutableList<UUID> = Lists.newArrayList()

    private var sourceDataSource: DataSource? = null
    private var destinationDataSource: DataSource? = null

    private var gcpProjectId: String? = null
    private var cloudSqlInstanceId: String? = null
    private var cloudSqlInstanceUsername: String? = null
    private var cloudSqlInstancePassword: String? = null
    private var cloudSqlInstancePublicIp: String? = null

    fun removeConnection(connection: UUID) {
      connectionIds.remove(connection)
    }

    init {
      // reads env vars to assign static variables
      assignEnvVars()

      if (isGke && !isKube) {
        throw RuntimeException("KUBE Flag should also be enabled if GKE flag is enabled")
      }
      if (!isGke) {
        sourcePsql = PostgreSQLContainer(SOURCE_POSTGRES_IMAGE_NAME)
        sourcePsql!!
          .withUsername(SOURCE_USERNAME)
          .withPassword(SOURCE_PASSWORD)
        sourcePsql!!.start()

        destinationPsql = PostgreSQLContainer(DESTINATION_POSTGRES_IMAGE_NAME)
        destinationPsql!!.start()
      } else {
        this.cloudSqlDatabaseProvisioner = CloudSqlDatabaseProvisioner()
        sourceDatabaseName =
          cloudSqlDatabaseProvisioner!!.createDatabase(
            gcpProjectId,
            cloudSqlInstanceId,
            generateRandomCloudSqlDatabaseName(),
          )
        destinationDatabaseName =
          cloudSqlDatabaseProvisioner!!.createDatabase(
            gcpProjectId,
            cloudSqlInstanceId,
            generateRandomCloudSqlDatabaseName(),
          )
      }

      val expectedSchema = Jsons.deserialize(EXPECTED_JSON_SCHEMA)
      val expectedStream =
        AirbyteStream(
          STREAM_NAME,
          expectedSchema,
          java.util.List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
          false,
          listOf(),
          listOf(),
          PUBLIC,
          true,
          null,
        )
      val expectedStreamConfig =
        AirbyteStreamConfiguration(
          SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          listOf(),
          null,
          listOf(),
          STREAM_NAME.replace(".", "_"),
          true,
          true,
          null,
          null,
          null,
          listOf(),
          listOf(),
          listOf(),
          null,
          null,
          null,
        )
      expectedAirbyteCatalog =
        AirbyteCatalog(
          java.util.List.of(
            AirbyteStreamAndConfiguration(expectedStream, expectedStreamConfig),
          ),
        )

      retryPolicy =
        RetryPolicy
          .builder<Any>()
          .handle(java.util.List.of<Class<out Throwable?>>(Exception::class.java))
          .withBackoff(Duration.ofSeconds(JITTER_MAX_INTERVAL_SECS.toLong()), Duration.ofSeconds(FINAL_INTERVAL_SECS.toLong()))
          .withMaxRetries(MAX_TRIES)
          .build()

      dataplaneGroupId =
        apiClient.dataplaneGroupApi
          .listDataplaneGroups(DataplaneGroupListRequestBody(DEFAULT_ORGANIZATION_ID))
          .dataplaneGroups!!
          .first()
          .dataplaneGroupId

      LOGGER.info("Using external deployment of airbyte.")
    }

    fun stopDbAndContainers() {
      if (isGke) {
        try {
          close(sourceDataSource)
          close(destinationDataSource)
        } catch (e: Exception) {
          LOGGER.warn("Failed to close data sources", e)
        }
      } else {
        sourcePsql!!.stop()
        destinationPsql!!.stop()
      }
    }

    @Throws(SQLException::class, URISyntaxException::class, IOException::class)
    fun setup() {
      if (isGke) {
        // Prepare the database data sources.
        LOGGER.info("postgresPassword: {}", cloudSqlInstancePassword)
        sourceDataSource =
          getDataSource(
            cloudSqlInstanceUsername!!,
            cloudSqlInstancePassword!!,
            cloudSqlInstancePublicIp!!,
            sourceDatabaseName!!,
          )
        destinationDataSource =
          getDataSource(
            cloudSqlInstanceUsername!!,
            cloudSqlInstancePassword!!,
            cloudSqlInstancePublicIp!!,
            destinationDatabaseName!!,
          )
        // seed database.
        runSqlScript(Path.of(MoreResources.readResourceAsFile(postgresSqlInitFile).toURI()), getSourceDatabase())
      } else {
        runSqlScript(MountableFile.forClasspathResource(postgresSqlInitFile!!), sourcePsql!!)

        sourceDataSource = createDataSource(sourcePsql!!)
        destinationDataSource = createDataSource(destinationPsql!!)

        // Pinning Postgres destination version. This doesn't work on GKE since the
        // airbyte-cron will revert this change. On GKE we are pinning the version by
        // adding an entry to the scoped_configuration table.
        val postgresDestDef = postgresDestinationDefinition
        if (postgresDestDef.dockerImageTag != POSTGRES_DESTINATION_CONNECTOR_VERSION) {
          LOGGER.info("Setting postgres destination connector to version {}...", POSTGRES_DESTINATION_CONNECTOR_VERSION)
          try {
            updateDestinationDefinitionVersion(postgresDestDef.destinationDefinitionId, POSTGRES_DESTINATION_CONNECTOR_VERSION)
          } catch (e: ClientException) {
            LOGGER.error("Error while updating destination definition version", e)
          } catch (e: ServerException) {
            LOGGER.error("Error while updating destination definition version", e)
          }
        }
      }
    }

    fun cleanup() {
      try {
        clearSourceDbData()
        clearDestinationDbData()
        for (operationId in operationIds) {
          deleteOperation(operationId)
        }

        for (connectionId in connectionIds) {
          disableConnection(connectionId)
        }

        for (sourceId in sourceIds) {
          deleteSource(sourceId)
        }

        // TODO(mfsiega-airbyte): clean up source definitions that get created.
        for (destinationId in destinationIds) {
          deleteDestination(destinationId)
        }
        if (isGke) {
          close(sourceDataSource)
          close(destinationDataSource)

          cloudSqlDatabaseProvisioner!!.deleteDatabase(
            gcpProjectId,
            cloudSqlInstanceId,
            sourceDatabaseName,
          )
          cloudSqlDatabaseProvisioner!!.deleteDatabase(
            gcpProjectId,
            cloudSqlInstanceId,
            destinationDatabaseName,
          )
        } else {
          destinationPsql!!.stop()
          sourcePsql!!.stop()
        }
        // TODO(mfsiega-airbyte): clean up created source definitions.
      } catch (e: Exception) {
        LOGGER.error("Error tearing down test fixtures", e)
      }
    }

    /**
     * This method is intended to be called at the beginning of a new test run - it identifies and
     * disables any pre-existing scheduled connections that could potentially interfere with a new test
     * run.
     */
    fun ensureCleanSlate() {
      if (!ensureCleanSlate) {
        LOGGER.info("proceeding without cleaning up pre-existing connections.")
        return
      }
      LOGGER.info("ENSURE_CLEAN_SLATE was true, disabling all scheduled connections using postgres source or postgres destination...")
      try {
        val sourceDefinitionId = postgresSourceDefinitionId
        val destinationDefinitionId = postgresDestinationDefinitionId

        val sourceDefinitionConnections =
          apiClient.connectionApi
            .listConnectionsByActorDefinition(
              ActorDefinitionRequestBody(sourceDefinitionId, ActorType.SOURCE),
            ).connections

        val destinationDefinitionConnections =
          apiClient.connectionApi
            .listConnectionsByActorDefinition(
              ActorDefinitionRequestBody(destinationDefinitionId, ActorType.DESTINATION),
            ).connections

        val allConnections: MutableSet<ConnectionRead> = Sets.newHashSet()
        allConnections.addAll(sourceDefinitionConnections)
        allConnections.addAll(destinationDefinitionConnections)

        val allConnectionsToDisable =
          allConnections
            .stream() // filter out any connections that aren't active
            // filter out any manual connections, since we only want to disable scheduled syncs
            .filter { connection: ConnectionRead -> connection.status == ConnectionStatus.ACTIVE }
            .filter { connection: ConnectionRead -> connection.scheduleType != ConnectionScheduleType.MANUAL }
            .toList()

        LOGGER.info("Found {} existing connection(s) to clean up", allConnectionsToDisable.size)
        if (!allConnectionsToDisable.isEmpty()) {
          for ((connectionId) in allConnectionsToDisable) {
            disableConnection(connectionId)
            LOGGER.info("disabled connection with ID {}", connectionId)
          }
        }
        LOGGER.info("ensureCleanSlate completed!")
      } catch (e: Exception) {
        LOGGER.warn("An exception occurred while ensuring a clean slate. Proceeding, but a clean slate is not guaranteed for this run.", e)
      }
    }

    private fun assignEnvVars() {
      isKube = System.getenv().containsKey("KUBE")
      isMinikube = System.getenv().containsKey("IS_MINIKUBE")
      isGke = System.getenv().containsKey("IS_GKE")
      isCI = System.getenv().containsKey("CI")
      isMac = System.getProperty("os.name").startsWith("Mac")
      ensureCleanSlate = System.getenv("ENSURE_CLEAN_SLATE") != null &&
        System.getenv("ENSURE_CLEAN_SLATE").equals("true", ignoreCase = true)
      gcpProjectId = System.getenv("GCP_PROJECT_ID")
      cloudSqlInstanceId = System.getenv("CLOUD_SQL_INSTANCE_ID")
      cloudSqlInstanceUsername = System.getenv("CLOUD_SQL_INSTANCE_USERNAME")
      cloudSqlInstancePassword = System.getenv("CLOUD_SQL_INSTANCE_PASSWORD")
      cloudSqlInstancePublicIp = System.getenv("CLOUD_SQL_INSTANCE_PUBLIC_IP")
    }

    private val workflowClient: WorkflowClient
      get() {
        val temporalUtils =
          TemporalUtils(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Optional.empty(),
          )
        val temporalService =
          temporalUtils.createTemporalService(
            TemporalWorkflowUtils.getAirbyteTemporalOptions(
              TEMPORAL_HOST,
              TemporalSdkTimeouts(),
            ),
            TemporalUtils.DEFAULT_NAMESPACE,
          )
        return WorkflowClient.newInstance(temporalService)
      }

    fun getWorkflowState(connectionId: UUID): WorkflowState {
      val workflowCLient = workflowClient

      // check if temporal workflow is reachable
      val connectionManagerWorkflow =
        workflowCLient.newWorkflowStub(ConnectionManagerWorkflow::class.java, "connection_manager_$connectionId")
      return connectionManagerWorkflow.getState()
    }

    fun terminateTemporalWorkflow(connectionId: UUID) {
      val workflowCLient = workflowClient

      // check if temporal workflow is reachable
      getWorkflowState(connectionId)

      // Terminate workflow
      LOGGER.info("Terminating temporal workflow...")
      workflowCLient.newUntypedWorkflowStub("connection_manager_$connectionId").terminate("")

      // remove connection to avoid exception during tear down
      connectionIds.remove(connectionId)
    }

    @Throws(IOException::class)
    fun discoverSourceSchema(sourceId: UUID): AirbyteCatalog? = discoverSourceSchemaWithId(sourceId).catalog

    @Throws(IOException::class)
    fun discoverSourceSchemaWithId(sourceId: UUID): SourceDiscoverSchemaRead =
      Failsafe.with(retryPolicy).get(
        CheckedSupplier {
          val result =
            apiClient.sourceApi.discoverSchemaForSource(SourceDiscoverSchemaRequestBody(sourceId, null, true, null))
          if (result.catalog == null) {
            throw RuntimeException("no catalog returned, retrying... (result was $result)")
          }
          result
        },
      )

    // Run check Connection workflow.
    @Throws(IOException::class)
    fun checkConnection(sourceId: UUID) {
      apiClient.sourceApi.checkConnectionToSource(SourceIdRequestBody(sourceId))
    }

    @Throws(IOException::class)
    fun discoverSourceSchemaWithoutCache(sourceId: UUID): AirbyteCatalog? =
      apiClient.sourceApi
        .discoverSchemaForSource(
          SourceDiscoverSchemaRequestBody(sourceId, null, true, null),
        ).catalog

    @Throws(IOException::class)
    fun getDestinationDefinitionSpec(
      destinationDefinitionId: UUID,
      workspaceId: UUID,
    ): DestinationDefinitionSpecificationRead =
      apiClient.destinationDefinitionSpecificationApi
        .getDestinationDefinitionSpecification(
          DestinationDefinitionIdWithWorkspaceId(destinationDefinitionId, workspaceId),
        )

    @Throws(IOException::class)
    fun getSourceDefinitionSpec(
      sourceDefinitionId: UUID,
      workspaceId: UUID,
    ): SourceDefinitionSpecificationRead =
      apiClient.sourceDefinitionSpecificationApi
        .getSourceDefinitionSpecification(
          SourceDefinitionIdWithWorkspaceId(sourceDefinitionId, workspaceId),
        )

    fun getSourceDatabase(): Database = sourceDataSource?.let { getDatabase(it) } ?: throw IllegalStateException("Source database is not initialized")

    fun getDestinationDatabase(): Database =
      destinationDataSource?.let { getDatabase(it) } ?: throw IllegalStateException("Destination database is not initialized")

    fun getDatabase(dataSource: DataSource): Database = Database(createDslContext(dataSource, SQLDialect.POSTGRES))

    /**
     * Assert that the normalized destination matches the input records, only expecting a single id
     * column.
     *
     * @param expectedRecords the records that we expect
     * @throws Exception while retrieving sources
     */
    @Throws(Exception::class)
    fun assertNormalizedDestinationContainsIdColumn(
      outputSchema: String?,
      expectedRecords: List<JsonNode>,
    ) {
      val destination = getSourceDatabase()
      val finalDestinationTable = String.format("%s.%s%s", outputSchema, OUTPUT_STREAM_PREFIX, STREAM_NAME.replace(".", "_"))
      val destinationRecords = retrieveRecordsFromDatabase(destination, finalDestinationTable)

      Assertions.assertEquals(
        expectedRecords.size,
        destinationRecords.size,
        String.format("source contains: %s record. destination contains: %s", expectedRecords.size, destinationRecords.size),
      )

      // Assert that each expected record id is present in the actual records.
      for (expectedRecord in expectedRecords) {
        Assertions.assertTrue(
          destinationRecords
            .stream()
            .anyMatch { r: JsonNode -> r[COLUMN_ID].asInt() == expectedRecord[COLUMN_ID].asInt() },
          String.format("destination does not contain record:\n %s \n destination contains:\n %s\n", expectedRecord, destinationRecords),
        )
      }

      for (actualRecord in destinationRecords) {
        val fieldNamesIterator = actualRecord.fieldNames()
        while (fieldNamesIterator.hasNext()) {
          val fieldName = fieldNamesIterator.next()
          // NOTE: we filtered this column out, so we check that it isn't present.
          Assertions.assertNotEquals(fieldName, COLUMN_NAME)
        }
      }
    }

    @Throws(URISyntaxException::class, SQLException::class, IOException::class)
    fun runSqlScriptInSource(resourceName: String) {
      LOGGER.debug("Running sql script in source: {}", resourceName)
      if (isGke) {
        runSqlScript(Path.of(MoreResources.readResourceAsFile(resourceName).toURI()), getSourceDatabase())
      } else {
        runSqlScript(MountableFile.forClasspathResource(resourceName), sourcePsql!!)
      }
    }

    @Throws(Exception::class)
    fun createConnection(create: TestConnectionCreate): ConnectionRead {
        /*
         * We control the name inside this method to avoid collisions of sync name and namespace. Especially
         * if namespaces collide. This can cause tests to flake as they will be writing to the same tables
         * in the destination.
         */

      val slug = RandomStringUtils.randomAlphabetic(5).lowercase(Locale.getDefault())
      val name = "accp-test-connection-" + slug + (if (create.nameSuffix != null) "-" + create.nameSuffix else "")
      val namespace = "accp_test_$slug"

      return createConnectionFromRequest(
        ConnectionCreate(
          create.srcId,
          create.dstId,
          ConnectionStatus.ACTIVE,
          name,
          NamespaceDefinitionType.CUSTOMFORMAT,
          namespace,
          OUTPUT_STREAM_PREFIX,
          create.operationIds,
          create.configuredCatalog,
          null,
          create.scheduleType,
          create.scheduleData,
          null,
          create.catalogId,
          null,
          create.dataplaneGroupId,
          null,
          null,
          null,
          null,
          null,
        ),
      )
    }

    @Throws(Exception::class)
    fun createConnectionSourceNamespace(create: TestConnectionCreate): ConnectionRead {
        /*
         * We control the name inside this method to avoid collisions of sync name and namespace. Especially
         * if namespaces collide. This can cause tests to flake as they will be writing to the same tables
         * in the destination.
         */

      val slug = RandomStringUtils.randomAlphabetic(5).lowercase(Locale.getDefault())
      val name = "accp-test-connection-" + slug + (if (create.nameSuffix != null) "-" + create.nameSuffix else "")
      val namespace = "accp_test_$slug"

      return createConnectionFromRequest(
        ConnectionCreate(
          create.srcId,
          create.dstId,
          ConnectionStatus.ACTIVE,
          name,
          NamespaceDefinitionType.CUSTOMFORMAT,
          namespace + "_\${SOURCE_NAMESPACE}",
          OUTPUT_STREAM_PREFIX,
          create.operationIds,
          create.configuredCatalog,
          null,
          create.scheduleType,
          create.scheduleData,
          null,
          create.catalogId,
          null,
          create.dataplaneGroupId,
          null,
          null,
          null,
          null,
          null,
        ),
      )
    }

    @Throws(IOException::class)
    private fun createConnectionFromRequest(request: ConnectionCreate): ConnectionRead {
      val connection = apiClient.connectionApi.createConnection(request)
      connectionIds.add(connection.connectionId)
      return connection
    }

    @Throws(IOException::class)
    fun getConnection(connectionId: UUID): ConnectionRead = apiClient.connectionApi.getConnection(ConnectionIdRequestBody(connectionId))

    @Throws(Exception::class)
    fun updateConnectionSchedule(
      connectionId: UUID,
      newScheduleType: ConnectionScheduleType?,
      newScheduleData: ConnectionScheduleData?,
    ) {
      updateConnection(
        ConnectionUpdate(
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
          null,
        ),
      )
    }

    @Throws(IOException::class, InterruptedException::class)
    fun updateConnectionCatalog(
      connectionId: UUID,
      catalog: AirbyteCatalog?,
    ) {
      updateConnection(
        ConnectionUpdate(
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
          null,
        ),
      )
    }

    @Throws(IOException::class, InterruptedException::class)
    fun updateConnectionSourceCatalogId(
      connectionId: UUID,
      sourceCatalogId: UUID?,
    ): ConnectionRead =
      updateConnection(
        ConnectionUpdate(
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
          null,
        ),
      )

    @Throws(IOException::class, InterruptedException::class)
    private fun updateConnection(request: ConnectionUpdate): ConnectionRead {
      val result = apiClient.connectionApi.updateConnection(request)
      // Attempting to sync immediately after updating the connection can run into a race condition in the
      // connection manager workflow hangs. This should be fixed in the backend, but for now we try to
      // tolerate it.
      Thread.sleep((1000 * 5).toLong())
      return result
    }

    @Throws(IOException::class)
    fun syncConnection(connectionId: UUID): JobInfoRead = apiClient.connectionApi.syncConnection(ConnectionIdRequestBody(connectionId))

    @Throws(IOException::class)
    fun cancelSync(jobId: Long): JobInfoRead = apiClient.jobsApi.cancelJob(JobIdRequestBody(jobId))

    @Throws(IOException::class)
    fun resetConnection(connectionId: UUID): JobInfoRead = apiClient.connectionApi.resetConnection(ConnectionIdRequestBody(connectionId))

    @Throws(IOException::class)
    fun deleteConnection(connectionId: UUID) {
      apiClient.connectionApi.deleteConnection(ConnectionIdRequestBody(connectionId))
    }

    @JvmOverloads
    @Throws(IOException::class)
    fun createPostgresDestination(workspaceId: UUID = defaultWorkspaceId): DestinationRead =
      createDestination(
        "AccTestDestination-" + UUID.randomUUID(),
        workspaceId,
        postgresDestinationDefinitionId,
        getDestinationDbConfig(),
      )

    @Throws(IOException::class)
    fun createDestination(
      name: String,
      workspaceId: UUID,
      destinationDefId: UUID,
      destinationConfig: JsonNode,
    ): DestinationRead {
      val destination =
        apiClient.destinationApi.createDestination(
          DestinationCreate(
            workspaceId,
            name,
            destinationDefId,
            destinationConfig,
            null,
          ),
        )
      destinationIds.add(destination.destinationId)
      return destination
    }

    @Throws(IOException::class)
    fun updateDestination(
      destinationId: UUID,
      updatedConfig: JsonNode,
      name: String,
    ): DestinationRead {
      val destinationUpdate =
        DestinationUpdate(
          destinationId,
          updatedConfig,
          name,
          null,
        )

      val checkResponse = apiClient.destinationApi.checkConnectionToDestinationForUpdate(destinationUpdate)
      if (checkResponse.status != CheckConnectionRead.Status.SUCCEEDED) {
        throw RuntimeException("Check connection failed: " + checkResponse.message)
      }

      return Failsafe.with(retryPolicy).get(
        CheckedSupplier { apiClient.destinationApi.updateDestination(destinationUpdate) },
      )
    }

    @Throws(IOException::class)
    fun checkDestination(destinationId: UUID): CheckConnectionRead.Status? =
      apiClient.destinationApi
        .checkConnectionToDestination(DestinationIdRequestBody(destinationId))
        .status

    @Throws(Exception::class)
    fun createDbtCloudWebhookOperation(
      workspaceId: UUID,
      webhookConfigId: UUID?,
    ): OperationRead =
      apiClient.operationApi.createOperation(
        OperationCreate(
          workspaceId,
          "reqres test",
          OperatorConfiguration(
            OperatorType.WEBHOOK,
            OperatorWebhook(
              webhookConfigId,
              OperatorWebhook.WebhookType.DBT_CLOUD,
              OperatorWebhookDbtCloud(123, 456),
              null,
              null,
            ),
          ),
        ),
      )

    @Throws(SQLException::class)
    fun retrieveRecordsFromDatabase(
      database: Database,
      table: String?,
    ): List<JsonNode> =
      database
        .query { context: DSLContext ->
          context.fetch(
            String.format(
              "SELECT * FROM %s;",
              table,
            ),
          )
        }.stream()
        .map { obj: Record -> obj.intoMap() }
        .map { `object`: Map<String, Any>? -> Jsons.jsonNode(`object`) }
        .collect(Collectors.toList())

    fun getSourceDbConfig(): JsonNode = getDbConfig(sourcePsql, false, false, sourceDatabaseName)

    fun getSourceDbConfigWithHiddenPassword(): JsonNode = getDbConfig(sourcePsql, true, false, sourceDatabaseName)

    fun getDestinationDbConfig(): JsonNode = getDbConfig(destinationPsql, false, true, destinationDatabaseName)

    fun getDestinationDbConfigWithHiddenPassword(): JsonNode = getDbConfig(destinationPsql, true, true, destinationDatabaseName)

    fun getDbConfig(
      psql: PostgreSQLContainer<*>?,
      hiddenPassword: Boolean,
      withSchema: Boolean,
      databaseName: String?,
    ): JsonNode {
      try {
        val dbConfig =
          if (isKube && isGke) {
            dbConfig(
              if (hiddenPassword) null else cloudSqlInstancePassword,
              withSchema,
              cloudSqlInstanceUsername!!,
              cloudSqlInstancePublicIp!!,
              databaseName,
            )
          } else {
            localConfig(psql!!, hiddenPassword, withSchema)
          }
        val config = Jsons.jsonNode(dbConfig)
        LOGGER.info("Using db config: {}", Jsons.toPrettyString(config))
        return config
      } catch (e: Exception) {
        throw RuntimeException(e)
      }
    }

    private fun localConfig(
      psql: PostgreSQLContainer<*>,
      hiddenPassword: Boolean,
      withSchema: Boolean,
    ): Map<Any, Any> {
      val dbConfig: MutableMap<Any, Any> = HashMap()
      if (isCI && !isGke) {
        dbConfig[JdbcUtils.HOST_KEY] = psql.host
      } else {
        dbConfig[JdbcUtils.HOST_KEY] = hostname
      }

      if (hiddenPassword) {
        dbConfig[JdbcUtils.PASSWORD_KEY] = "**********"
      } else {
        dbConfig[JdbcUtils.PASSWORD_KEY] = psql.password
      }

      dbConfig[JdbcUtils.PORT_KEY] = psql.firstMappedPort
      dbConfig[JdbcUtils.DATABASE_KEY] = psql.databaseName
      dbConfig[JdbcUtils.USERNAME_KEY] = psql.username
      dbConfig[JdbcUtils.SSL_KEY] = false

      if (withSchema) {
        dbConfig[JdbcUtils.SCHEMA_KEY] = "public"
      }
      return dbConfig
    }

    val hostname: String
      get() {
        if (isKube) {
          if (KUBE_PROCESS_RUNNER_HOST != "") {
            return KUBE_PROCESS_RUNNER_HOST
          }
          return if (isMinikube) {
            // used with minikube driver=none instance
            try {
              Inet4Address.getLocalHost().hostAddress
            } catch (e: UnknownHostException) {
              throw RuntimeException(e)
            }
          } else {
            // used on a single node with docker driver
            "host.docker.internal"
          }
        } else if (isMac) {
          return "host.docker.internal"
        } else {
          return "localhost"
        }
      }

    @Throws(IOException::class)
    fun getJobInfoRead(id: Long): JobInfoRead = apiClient.jobsApi.getJobInfo(JobIdRequestBody(id))

    @Throws(IOException::class)
    fun createE2eSourceDefinition(workspaceId: UUID?): SourceDefinitionRead {
      val sourceDefinitionRead =
        apiClient.sourceDefinitionApi.createCustomSourceDefinition(
          CustomSourceDefinitionCreate(
            SourceDefinitionCreate(
              "E2E Test Source",
              "airbyte/source-e2e-test",
              SOURCE_E2E_TEST_CONNECTOR_VERSION,
              URI.create("https://example.com"),
              null,
              null,
            ),
            workspaceId,
            null,
            null,
          ),
        )
      sourceDefinitionIds.add(sourceDefinitionRead.sourceDefinitionId)
      return sourceDefinitionRead
    }

    @Throws(IOException::class)
    fun createPostgresSourceDefinition(
      workspaceId: UUID?,
      dockerImageTag: String,
    ): SourceDefinitionRead {
      val sourceDefinitionRead =
        apiClient.sourceDefinitionApi.createCustomSourceDefinition(
          CustomSourceDefinitionCreate(
            SourceDefinitionCreate(
              "Custom Postgres Source",
              "airbyte/source-postgres",
              dockerImageTag,
              URI.create("https://example.com"),
              null,
              null,
            ),
            workspaceId,
            null,
            null,
          ),
        )
      sourceDefinitionIds.add(sourceDefinitionRead.sourceDefinitionId)
      return sourceDefinitionRead
    }

    @Throws(IOException::class)
    fun createE2eDestinationDefinition(workspaceId: UUID?): DestinationDefinitionRead =
      Failsafe.with(retryPolicy).get(
        CheckedSupplier {
          apiClient.destinationDefinitionApi
            .createCustomDestinationDefinition(
              CustomDestinationDefinitionCreate(
                DestinationDefinitionCreate(
                  "E2E Test Destination",
                  "airbyte/destination-e2e-test",
                  DESTINATION_E2E_TEST_CONNECTOR_VERSION,
                  URI.create("https://example.com"),
                  null,
                  null,
                ),
                workspaceId,
                null,
                null,
              ),
            )
        },
      )

    @JvmOverloads
    @Throws(IOException::class)
    fun createPostgresSource(workspaceId: UUID = defaultWorkspaceId): SourceRead =
      createSource(
        "acceptanceTestDb-" + UUID.randomUUID(),
        workspaceId,
        postgresSourceDefinitionId,
        getSourceDbConfig(),
      )

    fun createSource(
      name: String,
      workspaceId: UUID,
      sourceDefId: UUID,
      sourceConfig: JsonNode,
    ): SourceRead {
      val source =
        Failsafe.with(retryPolicy).get(
          CheckedSupplier {
            apiClient.sourceApi.createSource(
              SourceCreate(
                sourceDefId,
                sourceConfig,
                workspaceId,
                name,
                null,
                null,
              ),
            )
          },
        )
      sourceIds.add(source.sourceId)
      return source
    }

    @Throws(IOException::class)
    fun updateSource(
      sourceId: UUID,
      updatedConfig: JsonNode,
      name: String,
    ): SourceRead {
      val sourceUpdate =
        SourceUpdate(
          sourceId,
          updatedConfig,
          name,
          null,
          null,
        )

      val checkResponse = apiClient.sourceApi.checkConnectionToSourceForUpdate(sourceUpdate)
      if (checkResponse.status != CheckConnectionRead.Status.SUCCEEDED) {
        throw RuntimeException("Check connection failed: " + checkResponse.message)
      }

      return Failsafe.with(retryPolicy).get(
        CheckedSupplier { apiClient.sourceApi.updateSource(sourceUpdate) },
      )
    }

    fun checkSource(sourceId: UUID): CheckConnectionRead =
      Failsafe.with(retryPolicy).get(
        CheckedSupplier { apiClient.sourceApi.checkConnectionToSource(SourceIdRequestBody(sourceId)) },
      )

    val postgresSourceDefinitionId: UUID
      get() =
        Failsafe
          .with(retryPolicy)
          .get(
            CheckedSupplier {
              apiClient.sourceDefinitionApi
                .listSourceDefinitions()
                .sourceDefinitions
                .stream()
                .filter { sourceRead: SourceDefinitionRead ->
                  "postgres".equals(
                    sourceRead.name,
                    ignoreCase = true,
                  )
                }.findFirst()
                .orElseThrow()
                .sourceDefinitionId
            },
          )

    @get:Throws(IOException::class)
    val postgresDestinationDefinitionId: UUID
      get() = postgresDestinationDefinition.destinationDefinitionId

    val postgresDestinationDefinition: DestinationDefinitionRead
      get() =
        Failsafe.with(retryPolicy).get(
          CheckedSupplier {
            apiClient.destinationDefinitionApi
              .listDestinationDefinitions()
              .destinationDefinitions
              .stream()
              .filter { destRead: DestinationDefinitionRead -> "postgres".equals(destRead.name, ignoreCase = true) }
              .findFirst()
              .orElseThrow()
          },
        )

    @Throws(IOException::class)
    fun updateDestinationDefinitionVersion(
      destinationDefinitionId: UUID,
      dockerImageTag: String,
    ) {
      Failsafe.with(retryPolicy).run(
        CheckedRunnable {
          apiClient.destinationDefinitionApi.updateDestinationDefinition(
            DestinationDefinitionUpdate(
              destinationDefinitionId,
              dockerImageTag,
              defaultWorkspaceId,
              null,
              null,
            ),
          )
        },
      )
    }

    @Throws(IOException::class)
    fun updateSourceDefinitionVersion(
      sourceDefinitionId: UUID,
      dockerImageTag: String,
    ) {
      apiClient.sourceDefinitionApi.updateSourceDefinition(
        SourceDefinitionUpdate(
          sourceDefinitionId,
          dockerImageTag,
          defaultWorkspaceId,
          null,
          null,
        ),
      )
    }

    @Throws(SQLException::class)
    private fun clearSourceDbData() {
      val database = getSourceDatabase()
      val pairs = listAllTables(database)
      for (pair in pairs) {
        LOGGER.debug("Clearing table {} {}", pair.schemaName, pair.tableName)
        database.query { context: DSLContext ->
          context.execute(
            java.lang.String.format(
              "DROP TABLE %s.%s",
              pair.schemaName,
              pair.tableName,
            ),
          )
        }
      }
    }

    @Throws(SQLException::class)
    private fun clearDestinationDbData() {
      val database = getDestinationDatabase()
      val pairs = listAllTables(database)
      for (pair in pairs) {
        LOGGER.debug("Clearing table {} {}", pair.schemaName, pair.tableName)
        database.query { context: DSLContext ->
          context.execute(
            java.lang.String.format(
              "DROP TABLE %s.%s CASCADE",
              pair.schemaName,
              pair.tableName,
            ),
          )
        }
      }
    }

    @Throws(Exception::class)
    private fun disableConnection(connectionId: UUID) {
      val connectionUpdate =
        ConnectionUpdate(
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
          null,
        )
      updateConnection(connectionUpdate)
    }

    private fun deleteSource(sourceId: UUID) {
      Failsafe.with(retryPolicy).run(CheckedRunnable { apiClient.sourceApi.deleteSource(SourceIdRequestBody(sourceId)) })
    }

    private fun deleteDestination(destinationId: UUID) {
      Failsafe.with(retryPolicy).run(CheckedRunnable { apiClient.destinationApi.deleteDestination(DestinationIdRequestBody(destinationId)) })
    }

    private fun deleteOperation(destinationId: UUID) {
      Failsafe.with(retryPolicy).run(CheckedRunnable { apiClient.operationApi.deleteOperation(OperationIdRequestBody(destinationId)) })
    }

    /**
     * Returns the most recent job for the provided connection.
     */
    fun getMostRecentSyncForConnection(connectionId: UUID): JobRead =
      Failsafe.with(retryPolicy).get(
        CheckedSupplier<JobRead> {
          apiClient.jobsApi
            .listJobsFor(
              JobListRequestBody(
                java.util.List.of(JobConfigType.SYNC),
                connectionId.toString(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
              ),
            ).jobs
            .stream()
            .findFirst()
            .map(JobWithAttemptsRead::job)
            .orElseThrow()
        },
      )

    @Throws(InterruptedException::class, IOException::class)
    fun waitForSuccessfulJob(originalJob: JobRead) {
      val job = waitWhileJobHasStatus(originalJob, Sets.newHashSet(JobStatus.PENDING, JobStatus.RUNNING, JobStatus.INCOMPLETE))

      val debugInfo = ArrayList<String?>()

      if (JobStatus.SUCCEEDED != job.status) {
        // If a job failed during testing, show us why.
        val id = JobIdRequestBody(originalJob.id)
        for ((attempt, _, logs) in apiClient.jobsApi.getJobInfo(id).attempts) {
          val msg = (
            "Unsuccessful job attempt " + attempt.id +
              " with status " + job.status + " produced log output as follows: " + logs!!.logLines
          )
          LOGGER.warn(msg)
          debugInfo.add(msg)
        }
      }
      Assertions.assertEquals(JobStatus.SUCCEEDED, job.status, Strings.join(debugInfo, ", "))
      Thread.sleep(200)
    }

    @Throws(InterruptedException::class)
    fun waitWhileJobHasStatus(
      originalJob: JobRead,
      jobStatuses: Set<JobStatus?>,
    ): JobRead = waitWhileJobHasStatus(originalJob, jobStatuses, Duration.ofMinutes(12))

    @Throws(InterruptedException::class)
    fun waitWhileJobHasStatus(
      originalJob: JobRead,
      jobStatuses: Set<JobStatus?>,
      maxWaitTime: Duration,
    ): JobRead {
      var job = originalJob

      val waitStart = Instant.now()
      while (jobStatuses.contains(job.status)) {
        if (Duration.between(waitStart, Instant.now()).compareTo(maxWaitTime) > 0) {
          LOGGER.info("Max wait time of {} has been reached. Stopping wait.", maxWaitTime)
          break
        }
        Thread.sleep(1000)
        try {
          job = apiClient.jobsApi.getJobInfo(JobIdRequestBody(job.id)).job
        } catch (e: ClientException) {
          // TODO(mfsiega-airbyte): consolidate our polling/retrying logic.
          LOGGER.warn("error querying jobs api, retrying...")
        } catch (e: ServerException) {
          LOGGER.warn("error querying jobs api, retrying...")
        } catch (e: IOException) {
          LOGGER.warn("error querying jobs api, retrying...")
        }
        LOGGER.info("waiting: job id: {} config type: {} status: {}", job.id, job.configType, job.status)
      }
      return job
    }

    @Throws(IOException::class, InterruptedException::class)
    fun waitWhileJobIsRunning(
      job: JobRead,
      maxWaitTime: Duration,
    ) {
      val waitStart = Instant.now()
      var jobDebugInfoRead = apiClient.jobsApi.getJobDebugInfo(JobIdRequestBody(job.id))
      LOGGER.info("workflow state: {}", jobDebugInfoRead.workflowState)
      while (jobDebugInfoRead.workflowState != null && jobDebugInfoRead.workflowState!!.running) {
        if (Duration.between(waitStart, Instant.now()).compareTo(maxWaitTime) > 0) {
          LOGGER.info("Max wait time of {} has been reached. Stopping wait.", maxWaitTime)
          break
        }
        LOGGER.info("waiting: job id: {}, workflowState.isRunning is still true", job.id)
        Thread.sleep(1000)
        jobDebugInfoRead = apiClient.jobsApi.getJobDebugInfo(JobIdRequestBody(job.id))
      }
    }

    @Throws(IOException::class, InterruptedException::class)
    fun waitForConnectionState(connectionId: UUID): ConnectionState {
      var connectionState =
        Failsafe.with(retryPolicy).get(
          CheckedSupplier { apiClient.stateApi.getState(ConnectionIdRequestBody(connectionId)) },
        )
      var count = 0
      while (count < FINAL_INTERVAL_SECS && (connectionState.state == null || connectionState.state!!.isNull)) {
        LOGGER.info("fetching connection state. attempt: {}", count++)
        connectionState = apiClient.stateApi.getState(ConnectionIdRequestBody(connectionId))
        Thread.sleep(1000)
      }
      return connectionState
    }

    /**
     * Wait until the sync succeeds by polling the Jobs API.
     *
     *
     * NOTE: !!! THIS WILL POTENTIALLY POLL FOREVER !!! so make sure the calling code has a deadline;
     * for example, a test timeout.
     *
     *
     * TODO: re-work the collection of polling helpers we have here into a sane set that rely on test
     * timeouts instead of implementing their own deadline logic.
     */
    @Throws(Exception::class)
    fun waitForSuccessfulSyncNoTimeout(jobRead: JobRead) {
      var job = jobRead
      while (IN_PROGRESS_JOB_STATUSES.contains(job.status)) {
        job = getJobInfoRead(job.id).job
        LOGGER.info("waiting: job id: {} config type: {} status: {}", job.id, job.configType, job.status)
        Thread.sleep(3000)
      }
      Assertions.assertEquals(JobStatus.SUCCEEDED, job.status)
    }

    @Throws(Exception::class)
    fun waitUntilTheNextJobIsStarted(
      connectionId: UUID,
      previousJobId: Long,
    ): JobRead {
      var mostRecentSyncJob = getMostRecentSyncForConnection(connectionId)
      var count = 0
      while (count < MAX_ALLOWED_SECOND_PER_RUN && mostRecentSyncJob.id == previousJobId) {
        Thread.sleep(Duration.ofSeconds(1).toMillis())
        mostRecentSyncJob = getMostRecentSyncForConnection(connectionId)
        ++count
      }
      val exceeded120seconds = count >= MAX_ALLOWED_SECOND_PER_RUN
      if (exceeded120seconds) {
        // Fail because taking more than FINAL_INTERVAL_SECSseconds to start a job is not expected
        // Returning the current mostRecentSyncJob here could end up hiding some issues
        Assertions.fail<Any>("unable to find the next job within FINAL_INTERVAL_SECSseconds")
      }
      LOGGER.info("Time to run the job: $count")
      return mostRecentSyncJob
    }

    @Throws(IOException::class)
    fun getNonExistentResource() {
      apiClient.destinationDefinitionSpecificationApi
        .getDestinationDefinitionSpecification(
          DestinationDefinitionIdWithWorkspaceId(
            UUID.randomUUID(),
            UUID.randomUUID(),
          ),
        )
    }

    @Throws(IOException::class)
    fun getSourceDefinition(sourceDefinitionId: UUID): SourceDefinitionRead =
      Failsafe
        .with(retryPolicy)
        .get(CheckedSupplier { apiClient.sourceDefinitionApi.getSourceDefinition(SourceDefinitionIdRequestBody(sourceDefinitionId)) })

    fun getConnectionState(connectionId: UUID): ConnectionState =
      Failsafe.with(retryPolicy).get(
        CheckedSupplier { apiClient.stateApi.getState(ConnectionIdRequestBody(connectionId)) },
      )

    fun webBackendUpdateConnection(update: WebBackendConnectionUpdate) {
      Failsafe.with(retryPolicy).run(CheckedRunnable { apiClient.webBackendApi.webBackendUpdateConnection(update) })
    }

    fun listSyncsForWorkspaces(workspaceIds: List<UUID>): List<JobWithAttemptsRead> =
      Failsafe.with(retryPolicy).get(
        CheckedSupplier {
          apiClient.jobsApi
            .listJobsForWorkspaces(
              JobListForWorkspacesRequestBody(
                java.util.List.of(JobConfigType.SYNC),
                null,
                workspaceIds,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
              ),
            ).jobs
        },
      )

    fun listAllConnectionsForWorkspace(workspaceId: UUID): ConnectionReadList =
      Failsafe
        .with(retryPolicy)
        .get(CheckedSupplier { apiClient.connectionApi.listAllConnectionsForWorkspace(WorkspaceIdRequestBody(workspaceId, false)) })

    fun listSourcesForWorkspace(workspaceId: UUID): SourceReadList =
      Failsafe.with(retryPolicy).get(
        CheckedSupplier { apiClient.sourceApi.listSourcesForWorkspace(WorkspaceIdRequestBody(workspaceId, false)) },
      )

    fun listSourcesForWorkspacePaginated(workspaceIds: List<UUID>): SourceReadList =
      Failsafe.with(retryPolicy).get(
        CheckedSupplier {
          apiClient.sourceApi.listSourcesForWorkspacePaginated(
            ListResourcesForWorkspacesRequestBody(workspaceIds, Pagination(1000, 0), null, null),
          )
        },
      )

    fun deleteWorkspace(workspaceId: UUID) {
      Failsafe.with(retryPolicy).run(CheckedRunnable { apiClient.workspaceApi.deleteWorkspace(WorkspaceIdRequestBody(workspaceId, false)) })
    }

    fun deleteSourceDefinition(sourceDefinitionId: UUID) {
      Failsafe
        .with(retryPolicy)
        .run(CheckedRunnable { apiClient.sourceDefinitionApi.deleteSourceDefinition(SourceDefinitionIdRequestBody(sourceDefinitionId)) })
    }

    @Throws(IOException::class, InterruptedException::class)
    fun updateSchemaChangePreference(
      connectionId: UUID,
      nonBreakingChangesPreference: NonBreakingChangesPreference?,
      backfillPreference: SchemaChangeBackfillPreference?,
    ) {
      updateConnection(
        ConnectionUpdate(
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
          null,
        ),
      )
    }

    fun webBackendGetConnectionAndRefreshSchema(connectionId: UUID): WebBackendConnectionRead =
      Failsafe
        .with(retryPolicy)
        .get(CheckedSupplier { apiClient.webBackendApi.webBackendGetConnection(WebBackendConnectionRequestBody(connectionId, true)) })

    fun createWorkspaceWithId(workspaceId: UUID) {
      Failsafe.with<Any, RetryPolicy<Any>>(retryPolicy).run(
        CheckedRunnable {
          apiClient.workspaceApi
            .createWorkspaceIfNotExist(
              WorkspaceCreateWithId(
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
                null,
              ),
            )
        },
      )
    }

    fun getStreamStatuses(
      connectionId: UUID?,
      jobId: Long?,
      attempt: Int?,
      workspaceId: UUID,
    ): StreamStatusReadList =
      Failsafe.with(retryPolicy).get(
        CheckedSupplier {
          apiClient.streamStatusesApi.getStreamStatuses(
            StreamStatusListRequestBody(
              Pagination(100, 0),
              workspaceId,
              attempt,
              connectionId,
              jobId,
              null,
              null,
              null,
            ),
          )
        },
      )

    fun setIncrementalAppendSyncMode(
      airbyteCatalog: AirbyteCatalog,
      cursorField: List<String>?,
    ): AirbyteCatalog =
      AirbyteCatalog(
        airbyteCatalog.streams
          .stream()
          .map { stream: AirbyteStreamAndConfiguration ->
            AirbyteStreamAndConfiguration(
              stream.stream,
              AirbyteStreamConfiguration(
                SyncMode.INCREMENTAL,
                DestinationSyncMode.APPEND,
                cursorField,
                null,
                stream.config!!.primaryKey,
                stream.config!!.aliasName,
                stream.config!!.selected,
                stream.config!!.suggested,
                stream.config!!.destinationObjectName,
                stream.config!!.includeFiles,
                stream.config!!.fieldSelectionEnabled,
                stream.config!!.selectedFields,
                stream.config!!.hashedFields,
                stream.config!!.mappers,
                stream.config!!.minimumGenerationId,
                stream.config!!.generationId,
                stream.config!!.syncId,
              ),
            )
          }.collect(Collectors.toList()),
      )

    fun getUpdateInput(
      connection: ConnectionRead,
      catalog: AirbyteCatalog,
      operation: OperationRead,
    ): WebBackendConnectionUpdate {
      setIncrementalAppendSyncMode(catalog, java.util.List.of(COLUMN_ID))

      return WebBackendConnectionUpdate(
        connection.connectionId,
        connection.name,
        connection.namespaceDefinition,
        connection.namespaceFormat,
        connection.prefix,
        catalog,
        connection.schedule,
        null,
        null,
        connection.status,
        null,
        false,
        java.util.List.of(
          WebBackendOperationCreateOrUpdate(
            operation.workspaceId,
            operation.name,
            operation.operatorConfiguration,
            operation.operationId,
          ),
        ),
        connection.sourceCatalogId,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      )
    }

    fun compareCatalog(actual: AirbyteCatalog?) {
      Assertions.assertEquals(expectedAirbyteCatalog, actual)
    }

    fun <T> withFlag(
      flag: Flag<T>,
      context: Context?,
      value: T,
    ): FlagOverride<T> = testFlagsSetter!!.withFlag(flag, value, context)

    /**
     * Validates that job logs exist, are in the correct format and contain entries from various
     * participants. This method loops because not all participants may have reported by the time that a
     * job is marked as done.
     *
     * @param jobId The ID of the job associated with the job logs.
     * @param attemptNumber The attempt number of the job associated with the job logs.
     */
    fun validateLogs(
      jobId: Long,
      attemptNumber: Int,
    ) {
      val retryPolicy: RetryPolicy<Unit> =
        RetryPolicy
          .builder<Unit>()
          .handle(Exception::class.java, AssertionFailedError::class.java, org.opentest4j.AssertionFailedError::class.java)
          .withDelay(Duration.ofSeconds(5))
          .withMaxRetries(50)
          .build()
      try {
        Failsafe.with(retryPolicy).run(
          CheckedRunnable {
            // Assert that job logs exist
            val attempt =
              apiClient.attemptApi.getAttemptForJob(
                GetAttemptStatsRequestBody(jobId, attemptNumber),
              )
            // Structured logs should exist
            Assertions.assertEquals(LogFormatType.STRUCTURED, attempt.logType)
            Assertions.assertFalse(attempt.logs!!.events.isEmpty())
            Assertions.assertTrue(attempt.logs!!.logLines.isEmpty())
            // Assert that certain log lines are present in job logs to verify that all components can
            // contribute
            val logLines =
              listOf(
                "APPLY Stage: CLAIM", // workload launcher
                "----- START REPLICATION -----", // container orchestrator
              )
            validateLogLines(logLines, attempt)
          },
        )
      } catch (e: FailsafeException) {
        Assertions.fail<Any>("Failed to validate logs: retries exceeded waiting for logs.", e)
      }
    }

    fun validateLogLines(
      logLines: List<String>,
      attempt: AttemptInfoRead,
    ) {
      logLines.forEach(
        Consumer { logLine: String ->
          if (attempt.logs!!
              .events
              .stream()
              .noneMatch { e: LogEvent -> e.message.startsWith(logLine) }
          ) {
            Assertions.fail<Any>("Job logs do not contain any lines that start with '$logLine'.")
          }
        },
      )
    }

    companion object {
      private val LOGGER: Logger = LoggerFactory.getLogger(AcceptanceTestHarness::class.java)

      private val DESTINATION_POSTGRES_IMAGE_NAME: DockerImageName = DockerImageName.parse("postgres:15-alpine")

      private val SOURCE_POSTGRES_IMAGE_NAME: DockerImageName =
        DockerImageName
          .parse("debezium/postgres:15-alpine")
          .asCompatibleSubstituteFor("postgres")

      private val TEMPORAL_HOST: String = Optional.ofNullable(System.getenv("TEMPORAL_HOST")).orElse("temporal.airbyte.dev:80")

      private const val SOURCE_E2E_TEST_CONNECTOR_VERSION = "0.1.2"
      private const val DESTINATION_E2E_TEST_CONNECTOR_VERSION = "0.1.1"

      const val POSTGRES_DESTINATION_CONNECTOR_VERSION: String = "0.6.3"
      const val POSTGRES_SOURCE_LEGACY_CONNECTOR_VERSION: String = "0.4.26"

      const val OUTPUT_STREAM_PREFIX: String = "output_table_"
      private const val TABLE_NAME = "id_and_name"
      const val STREAM_NAME: String = TABLE_NAME
      const val COLUMN_ID: String = "id"
      const val COLUMN_NAME: String = "name"
      private const val SOURCE_USERNAME = "sourceusername"
      const val SOURCE_PASSWORD: String = "hunter2"
      const val PUBLIC_SCHEMA_NAME: String = "public"
      const val PUBLIC: String = "public"

      private const val DEFAULT_POSTGRES_INIT_SQL_FILE = "postgres_init.sql"

      const val JITTER_MAX_INTERVAL_SECS: Int = 10
      const val FINAL_INTERVAL_SECS: Int = 60
      const val MAX_TRIES: Int = 5
      const val MAX_ALLOWED_SECOND_PER_RUN: Int = 120

      private const val CLOUD_SQL_DATABASE_PREFIX = "acceptance_test_"

      // NOTE: we include `INCOMPLETE` here because the job may still retry; see
      // https://docs.airbyte.com/understanding-airbyte/jobs/.
      val IN_PROGRESS_JOB_STATUSES: Set<JobStatus> = java.util.Set.of(JobStatus.PENDING, JobStatus.INCOMPLETE, JobStatus.RUNNING)

      private val KUBE_PROCESS_RUNNER_HOST: String = Optional.ofNullable(System.getenv("KUBE_PROCESS_RUNNER_HOST")).orElse("")
      private val EXPECTED_JSON_SCHEMA =
        """
        {
          "type": "object",
          "properties": {
            "$COLUMN_NAME": {
              "type": "string"
            },
            "$COLUMN_ID": {
              "airbyte_type": "integer",
              "type": "number"
            }
          }
        }
        """.trimIndent()

      private var isKube = false
      private var isMinikube = false
      private var isGke = false
      private var isCI = false
      private var isMac = false
      private var ensureCleanSlate = false

      private fun generateRandomCloudSqlDatabaseName(): String = CLOUD_SQL_DATABASE_PREFIX + UUID.randomUUID()
    }
  }
