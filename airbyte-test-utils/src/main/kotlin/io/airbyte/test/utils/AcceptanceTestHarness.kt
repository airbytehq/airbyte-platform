/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import com.fasterxml.jackson.databind.JsonNode
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedRunnable
import dev.failsafe.function.CheckedSupplier
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStream
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.ConnectionCreate
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionState
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.ConnectionUpdate
import io.airbyte.api.client.model.generated.DataplaneGroupListRequestBody
import io.airbyte.api.client.model.generated.DestinationDefinitionRead
import io.airbyte.api.client.model.generated.DestinationDefinitionUpdate
import io.airbyte.api.client.model.generated.DestinationIdRequestBody
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.JobIdRequestBody
import io.airbyte.api.client.model.generated.JobInfoRead
import io.airbyte.api.client.model.generated.JobRead
import io.airbyte.api.client.model.generated.JobStatus
import io.airbyte.api.client.model.generated.NamespaceDefinitionType
import io.airbyte.api.client.model.generated.OperationIdRequestBody
import io.airbyte.api.client.model.generated.Pagination
import io.airbyte.api.client.model.generated.SourceIdRequestBody
import io.airbyte.api.client.model.generated.StreamStatusListRequestBody
import io.airbyte.api.client.model.generated.StreamStatusReadList
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.Resources
import io.airbyte.db.Database
import io.airbyte.db.factory.DataSourceFactory.close
import io.airbyte.db.jdbc.JdbcUtils
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.Flag
import io.airbyte.featureflag.tests.TestFlagsSetter
import io.airbyte.featureflag.tests.TestFlagsSetter.FlagOverride
import io.airbyte.test.utils.Databases.createDataSource
import io.airbyte.test.utils.Databases.createDslContext
import io.airbyte.test.utils.Databases.listAllTables
import io.airbyte.test.utils.GKEPostgresConfig.dbConfig
import io.airbyte.test.utils.GKEPostgresConfig.getDataSource
import io.airbyte.test.utils.GKEPostgresConfig.runSqlScript
import io.airbyte.test.utils.PostgreSQLContainerHelper.runSqlScript
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile
import java.io.IOException
import java.net.Inet4Address
import java.net.UnknownHostException
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.Locale
import java.util.UUID
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

/**
 * This class contains containers used for acceptance tests. Some of those containers/states are only used when the test are run without GKE.
 * Specific environmental variables govern what types of containers are run.
 *
 * This class is put in a separate module to be easily pulled in as a dependency for Airbyte Cloud Acceptance Tests.
 *
 * Containers and states include:
 *  * source postgres SQL
 *  * destination postgres SQL
 *  * kubernetes client
 *  * lists of UUIDS representing IDs of sources, destinations, connections, and operations
 */
class AcceptanceTestHarness(
  val apiClient: AirbyteApiClient,
  private val defaultWorkspaceId: UUID,
  private val postgresSqlInitFile: String? = DEFAULT_POSTGRES_INIT_SQL_FILE,
  val testFlagsSetter: TestFlagsSetter? = null,
) {
  val dataplaneGroupId: UUID

  private val expectedAirbyteCatalog: AirbyteCatalog
  private val retryPolicy: RetryPolicy<Any>

  private val isKube = System.getenv().containsKey("KUBE")
  private val isMinikube = System.getenv().containsKey("IS_MINIKUBE")
  private val isGke = System.getenv().containsKey("IS_GKE")
  private val isCI = System.getenv().containsKey("CI")
  private val isMac = System.getProperty("os.name").startsWith("Mac")
  private val ensureCleanSlate = System.getenv("ENSURE_CLEAN_SLATE")?.equals("true", ignoreCase = true) ?: false
  private val gcpProjectId = System.getenv("GCP_PROJECT_ID")
  private val cloudSqlInstanceId = System.getenv("CLOUD_SQL_INSTANCE_ID")
  private val cloudSqlInstanceUsername = System.getenv("CLOUD_SQL_INSTANCE_USERNAME")
  private val cloudSqlInstancePassword = System.getenv("CLOUD_SQL_INSTANCE_PASSWORD")
  private val cloudSqlInstancePublicIp = System.getenv("CLOUD_SQL_INSTANCE_PUBLIC_IP")

  /**
   * When the acceptance tests are run against a local instance of docker-compose or KUBE then these test containers are used.
   * When we run these tests in GKE, we spawn a source and destination postgres database ane use them for testing.
   */
  private var cloudSqlDatabaseProvisioner: CloudSqlDatabaseProvisioner? = null
  private var sourcePsql: PostgreSQLContainer<*>? = null
  private var destinationPsql: PostgreSQLContainer<*>? = null
  private var sourceDatabaseName: String? = null
  private var destinationDatabaseName: String? = null

  private val connectionIds = mutableListOf<UUID>()

  private var sourceDataSource: DataSource? = null
  private var destinationDataSource: DataSource? = null

  init {
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
        listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
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
        listOf(
          AirbyteStreamAndConfiguration(expectedStream, expectedStreamConfig),
        ),
      )

    retryPolicy =
      RetryPolicy
        .builder<Any>()
        .handle(listOf<Class<out Throwable?>>(Exception::class.java))
        .withBackoff(Duration.ofSeconds(JITTER_MAX_INTERVAL_SECS.toLong()), Duration.ofSeconds(FINAL_INTERVAL_SECS.toLong()))
        .withMaxRetries(MAX_TRIES)
        .build()

    dataplaneGroupId =
      apiClient.dataplaneGroupApi
        .listDataplaneGroups(DataplaneGroupListRequestBody(DEFAULT_ORGANIZATION_ID))
        .dataplaneGroups!!
        .first()
        .dataplaneGroupId

    log.info { "Using external deployment of airbyte." }
  }

  fun removeConnection(connection: UUID) {
    connectionIds.remove(connection)
  }

  fun stopDbAndContainers() {
    if (isGke) {
      try {
        close(sourceDataSource)
        close(destinationDataSource)
      } catch (e: Exception) {
        log.warn(e) { "Failed to close data sources" }
      }
    } else {
      sourcePsql!!.stop()
      destinationPsql!!.stop()
    }
  }

  fun setup() {
    if (isGke) {
      // Prepare the database data sources.
      sourceDataSource =
        getDataSource(
          cloudSqlInstanceUsername!!,
          cloudSqlInstancePassword!!,
          cloudSqlInstancePublicIp!!,
          sourceDatabaseName!!,
        )
      destinationDataSource =
        getDataSource(
          cloudSqlInstanceUsername,
          cloudSqlInstancePassword,
          cloudSqlInstancePublicIp,
          destinationDatabaseName!!,
        )
      // seed database.
      runSqlScript(Path.of(Resources.readResourceAsFile(postgresSqlInitFile!!).toURI()), getSourceDatabase())
    } else {
      runSqlScript(MountableFile.forClasspathResource(postgresSqlInitFile!!), sourcePsql!!)

      sourceDataSource = createDataSource(sourcePsql!!)
      destinationDataSource = createDataSource(destinationPsql!!)

      // Pinning Postgres destination version. This doesn't work on GKE since the
      // airbyte-cron will revert this change. On GKE we are pinning the version by
      // adding an entry to the scoped_configuration table.
      val postgresDestDef = postgresDestinationDefinition
      if (postgresDestDef.dockerImageTag != POSTGRES_DESTINATION_CONNECTOR_VERSION) {
        log.info { "Setting postgres destination connector to version $POSTGRES_DESTINATION_CONNECTOR_VERSION..." }
        try {
          updateDestinationDefinitionVersion(postgresDestDef.destinationDefinitionId, POSTGRES_DESTINATION_CONNECTOR_VERSION)
        } catch (e: ClientException) {
          log.error(e) { "Error while updating destination definition version" }
        } catch (e: ServerException) {
          log.error(e) { "Error while updating destination definition version" }
        }
      }
    }
  }

  private val postgresDestinationDefinition: DestinationDefinitionRead
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

  fun getSourceDatabase(): Database = sourceDataSource?.let { getDatabase(it) } ?: throw IllegalStateException("Source database is not initialized")

  fun getDestinationDatabase(): Database =
    destinationDataSource?.let { getDatabase(it) } ?: throw IllegalStateException("Destination database is not initialized")

  fun getDatabase(dataSource: DataSource): Database = Database(createDslContext(dataSource, SQLDialect.POSTGRES))

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

  private fun createConnectionFromRequest(request: ConnectionCreate): ConnectionRead {
    val connection = apiClient.connectionApi.createConnection(request)
    connectionIds.add(connection.connectionId)
    return connection
  }

  fun getConnection(connectionId: UUID): ConnectionRead = apiClient.connectionApi.getConnection(ConnectionIdRequestBody(connectionId))

  private fun updateConnection(request: ConnectionUpdate): ConnectionRead {
    val result = apiClient.connectionApi.updateConnection(request)
    // Attempting to sync immediately after updating the connection can run into a race condition in the
    // connection manager workflow hangs. This should be fixed in the backend, but for now we try to
    // tolerate it.
    Thread.sleep((1000 * 5).toLong())
    return result
  }

  fun syncConnection(connectionId: UUID): JobInfoRead = apiClient.connectionApi.syncConnection(ConnectionIdRequestBody(connectionId))

  fun deleteConnection(connectionId: UUID) {
    apiClient.connectionApi.deleteConnection(ConnectionIdRequestBody(connectionId))
  }

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

  fun getJobInfoRead(id: Long): JobInfoRead = apiClient.jobsApi.getJobInfo(JobIdRequestBody(id))

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

  private fun clearSourceDbData() {
    val database = getSourceDatabase()
    val pairs = listAllTables(database)
    for (pair in pairs) {
      log.debug { "Clearing table {} $pair.schemaName, pair.tableName" }
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

  private fun clearDestinationDbData() {
    val database = getDestinationDatabase()
    val pairs = listAllTables(database)
    for (pair in pairs) {
      log.debug { "Clearing table {} $pair.schemaName, pair.tableName" }
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

  private fun waitWhileJobHasStatus(
    originalJob: JobRead,
    jobStatuses: Set<JobStatus>,
    maxWaitTime: Duration,
  ): JobRead {
    var job = originalJob

    val waitStart = Instant.now()
    var logDebounce = 0
    while (jobStatuses.contains(job.status)) {
      logDebounce++
      if (Duration.between(waitStart, Instant.now()) > maxWaitTime) {
        log.info { "Max wait time of $maxWaitTime has been reached. Stopping wait." }
        break
      }
      Thread.sleep(1000)
      try {
        job = apiClient.jobsApi.getJobInfo(JobIdRequestBody(job.id)).job
      } catch (_: ClientException) {
        // TODO(mfsiega-airbyte): consolidate our polling/retrying logic.
        log.warn { "error querying jobs api, retrying..." }
      } catch (_: ServerException) {
        log.warn { "error querying jobs api, retrying..." }
      } catch (_: IOException) {
        log.warn { "error querying jobs api, retrying..." }
      }
      // if we are just waiting only log every 10 seconds to avoid spamming the logs.
      if (logDebounce % 10 == 0) {
        log.info { "waiting: job id: {} config type: {} status: $job.id, job.configType, job.status" }
      }
    }
    return job
  }

  fun getConnectionState(connectionId: UUID): ConnectionState =
    Failsafe.with(retryPolicy).get(
      CheckedSupplier { apiClient.stateApi.getState(ConnectionIdRequestBody(connectionId)) },
    )

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
            pagination = Pagination(100, 0),
            workspaceId = workspaceId,
            attemptNumber = attempt,
            connectionId = connectionId,
            jobId = jobId,
            jobType = null,
            streamName = null,
            streamNamespace = null,
          ),
        )
      },
    )

  // don't delete this even if it's unused!
  // this is only useful for testing feature flags, so if we do a good job of cleaning up flags,
  // this function should have no usages most of the time.
  // but we should keep it around regardless, so that we can always test flags easily.
  fun <T> withFlag(
    flag: Flag<T>,
    context: Context?,
    value: T,
  ): FlagOverride<T> = testFlagsSetter!!.withFlag(flag, value, context)

  companion object {
    private val DESTINATION_POSTGRES_IMAGE_NAME: DockerImageName = DockerImageName.parse("postgres:15-alpine")

    private val SOURCE_POSTGRES_IMAGE_NAME: DockerImageName =
      DockerImageName
        .parse("debezium/postgres:15-alpine")
        .asCompatibleSubstituteFor("postgres")

    private val TEMPORAL_HOST: String = System.getenv("TEMPORAL_HOST") ?: "temporal.airbyte.dev:80"

    const val POSTGRES_DESTINATION_CONNECTOR_VERSION: String = "0.6.3"

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

    private const val CLOUD_SQL_DATABASE_PREFIX = "acceptance_test_"

    private val KUBE_PROCESS_RUNNER_HOST: String = System.getenv("KUBE_PROCESS_RUNNER_HOST") ?: ""
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

    private fun generateRandomCloudSqlDatabaseName(): String = CLOUD_SQL_DATABASE_PREFIX + UUID.randomUUID()
  }
}
