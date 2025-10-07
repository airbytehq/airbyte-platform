/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.CheckConnectionRead
import io.airbyte.api.client.model.generated.ConnectionCreate
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionScheduleType
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.ConnectionUpdate
import io.airbyte.api.client.model.generated.ConnectorBuilderProjectIdWithWorkspaceId
import io.airbyte.api.client.model.generated.DataplaneGroupListRequestBody
import io.airbyte.api.client.model.generated.DeclarativeSourceDefinitionResponse
import io.airbyte.api.client.model.generated.DefinitionResponse
import io.airbyte.api.client.model.generated.DefinitionsResponse
import io.airbyte.api.client.model.generated.DestinationCatalog
import io.airbyte.api.client.model.generated.DestinationDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.DestinationDiscoverSchemaRequestBody
import io.airbyte.api.client.model.generated.DestinationIdRequestBody
import io.airbyte.api.client.model.generated.JobConfigType
import io.airbyte.api.client.model.generated.JobIdRequestBody
import io.airbyte.api.client.model.generated.JobListRequestBody
import io.airbyte.api.client.model.generated.JobStatus
import io.airbyte.api.client.model.generated.NamespaceDefinitionType
import io.airbyte.api.client.model.generated.NotificationSettings
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.client.model.generated.SourceDefinitionRead
import io.airbyte.api.client.model.generated.SourceDefinitionSpecificationRead
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRequestBody
import io.airbyte.api.client.model.generated.SourceIdRequestBody
import io.airbyte.api.client.model.generated.SourceRead
import io.airbyte.api.client.model.generated.UserAuthIdRequestBody
import io.airbyte.api.client.model.generated.UserIdRequestBody
import io.airbyte.api.client.model.generated.UserRead
import io.airbyte.api.client.model.generated.WebBackendConnectionRead
import io.airbyte.api.client.model.generated.WebBackendConnectionRequestBody
import io.airbyte.api.client.model.generated.WorkspaceCreate
import io.airbyte.api.client.model.generated.WorkspaceCreateRequest
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceResponse
import io.airbyte.api.client.model.generated.WorkspaceUpdateRequest
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.Flag
import io.airbyte.featureflag.tests.TestFlagsSetter
import io.airbyte.test.AirbyteApiClients
import io.airbyte.test.AtcConfig
import io.airbyte.test.PublicApiClient
import io.airbyte.test.connectionStatus
import io.airbyte.test.createAtcDestination
import io.airbyte.test.createAtcSource
import io.airbyte.test.createConnection
import io.airbyte.test.createConnectorBuilderProject
import io.airbyte.test.createDestinationDefinition
import io.airbyte.test.createSource
import io.airbyte.test.createSourceDefinition
import io.airbyte.test.createWorkspace
import io.airbyte.test.deleteConnection
import io.airbyte.test.deleteWorkspace
import io.airbyte.test.jobCancel
import io.airbyte.test.jobLogs
import io.airbyte.test.publicCreateDeclarativeSourceDefinition
import io.airbyte.test.publicCreateDestinationDefinition
import io.airbyte.test.publicCreateSourceDefinition
import io.airbyte.test.publicDeleteDestinationDefinition
import io.airbyte.test.publicDeleteSourceDefinition
import io.airbyte.test.publicUpdateDeclarativeSourceDefinition
import io.airbyte.test.publicUpdateDestinationDefinition
import io.airbyte.test.publicUpdateSourceDefinition
import io.airbyte.test.publishConnectorBuilderProject
import io.airbyte.test.syncConnection
import io.airbyte.test.updateAtcDestination
import io.airbyte.test.updateAtcSource
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

private val log = KotlinLogging.logger {}

/** All assets created by this helper will start with [NAME_PREFIX]. */
const val NAME_PREFIX = "ATC"

/** The Atc (Acceptance Test Connector) docker repo. */
const val CUSTOM_DOCKER_REPO = "coleairbyte/abcdk"

/** The Atc version. */
const val CUSTOM_DOCKER_TAG = "0.7.2"

/**
 * Main test helper class for Airbyte acceptance tests. This class provides a comprehensive
 * testing framework that manages the lifecycle of test resources and provides convenient
 * access to different API endpoints through specialized client wrappers.
 *
 * The AcceptanceTestHelper automatically handles resource cleanup and provides three main
 * API access points through inner classes:
 *
 * ## Usage Example:
 * ```kotlin
 * val helper = AcceptanceTestHelper("http://localhost:8000")
 * helper.setup() // Creates or uses existing workspace
 *
 * // Use admin APIs
 * val sourceId = helper.admin.createAtcSource()
 * val connectionId = helper.admin.createAtcConnection()
 *
 * // Use public APIs
 * val definitions = helper.public.fetchSourceDefinitions()
 *
 * // Use user APIs
 * val user = helper.user.getOrCreateUser()
 *
 * helper.tearDown() // Cleanup test resources
 * helper.tearDownAll() // Full cleanup including workspaces
 * ```
 *
 * ## Environment Variables:
 * This class uses the following environment variables for configuration:
 * - `AIRBYTE_SERVER_HOST`: The Airbyte server host URL (e.g., "http://localhost:8000").
 *   Used as the default host if not provided in the constructor.
 * - `AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID`: Optional workspace ID to use for tests.
 *   If not provided, a new workspace will be created automatically.
 *
 * ## Inner Classes:
 *
 * ### `admin`
 * Provides access to admin/internal APIs for managing connectors, connections, and workspace resources.
 * Use this for most test operations like creating sources, destinations, connections, and managing
 * sync jobs. The admin client has full privileges and can perform operations that regular users cannot.
 *
 * ### `public`
 * Provides access to the public API endpoints that external users would typically use. This includes
 * operations like fetching connector definitions, managing workspaces through public endpoints,
 * and working with declarative source definitions. Use this to test the public API surface.
 *
 * ### `user`
 * Provides access to user-scoped APIs that operate within the context of a specific user.
 * This includes user management operations and workspace creation with proper user context.
 * The user client is automatically configured with a test user email.
 *
 * ## Resource Management:
 * The helper automatically tracks all created resources (sources, destinations, connections, etc.)
 * and provides cleanup methods:
 * - `tearDown()`: Cleans up test resources but preserves workspaces
 * - `tearDownAll()`: Full cleanup including workspaces and users
 *
 * @param host The Airbyte server host URL. If null, defaults to AIRBYTE_SERVER_HOST environment variable.
 */
class AcceptanceTestClient(
  host: String? = System.getenv("AIRBYTE_SERVER_HOST"),
) {
  /** Host, defaults to AIRBYTE_SERVER_HOST env var. */
  val host: String =
    host?.takeIf { it.isNotBlank() }?.removeSuffix("/")
      ?: throw IllegalArgumentException("host must be provided or AIRBYTE_SERVER_HOST must be defined")

  /** Airbyte admin API client. */
  private val adminApiClient = AirbyteApiClients.adminApiClient(this.host)

  /** Airbyte public API client. */
  private val publicApiClient = AirbyteApiClients.publicApiClient(this.host)

  /** User email used by the user api client */
  private val userEmail = "acceptance-test-${now()}@airbyte.io"

  /** Airbyte user API client. */
  private val userApiClient = AirbyteApiClients.userApiClient(this.host, userEmail)

  /** All admin apis are accessible through this property. */
  val public: Public = Public(publicApiClient)

  /** All non-public Apis are accessible through this property. */
  val admin: Admin = Admin(adminApiClient)

  /** All user apis are accessible through this property. */
  val user: User = User(userApiClient)

  /** If this harness created a workspace, set this to true so we can properly cleanup. */
  private var workspaceCreated: Boolean = false

  /**
   * Workspace ID targeted by this harness.
   *
   * Some tests require a workspace to be created. In those cases, the workspace will be created
   * and the ID will be stored here. In other cases, the workspace will be created by the test
   * itself and the ID will be provided by the test. In the latter case, workspaceCreated will be
   * false.
   */
  lateinit var workspaceId: UUID
    private set

  private val testFFs = TestFlagsSetter(this.host)

  // only tracks additional workspaces created by callers of this harness
  private val workspaceIds = mutableSetOf<UUID>()
  private val srcDefIds = mutableSetOf<UUID>()
  private val dstDefIds = mutableSetOf<UUID>()
  private val pubSrcDefIds = mutableSetOf<UUID>()
  private val pubDecSrcDefIds = mutableSetOf<UUID>()
  private val pubDstDefIds = mutableSetOf<UUID>()
  private val srcIds = mutableSetOf<UUID>()
  private val dstIds = mutableSetOf<UUID>()
  private val conIds = mutableSetOf<UUID>()
  private val conBuilderIds = mutableSetOf<UUID>()
  private val userIds = mutableSetOf<UUID>()

  /**
   * Note: Do not remove this method even if it appears to be unused!
   * This method will only be used when testing feature-flags, which should be considered abnormal as feature-flags should have a short life-cycle.
   *
   * Executes a block of code with a specific feature flag value set for the given context.
   *
   * This method allows overriding the value of a specified feature flag temporarily within
   * the scope of the provided block of code. Once the block execution is complete,
   * the feature flag value reverts to its original state.
   *
   * @param flag The feature flag to override. Must be an instance of a class inheriting the `Flag` sealed class.
   * @param value The value to set for the feature flag during the execution of the block.
   * @param context A `Context` representation for feature flag evaluation, used for targeted overrides.
   *                If no context is provided, the flag applies globally within the scope of the block.
   * @param block A block of code to execute with the specified feature flag value.
   */
  fun <T> withFeatureFlag(
    flag: Flag<T>,
    value: T,
    context: Context? = null,
    block: () -> Unit,
  ) {
    testFFs
      .withFlag(
        flag = flag,
        value = value,
        context = context,
      ).use { block() }
  }

  private fun createWorkspace(): UUID =
    adminApiClient
      .createWorkspace()
      .also { log.info { "created workspace: $it" } }

  /**
   * Provides access to user-scoped APIs that operate within the context of a specific user.
   * Includes user management operations and workspace creation with proper user context.
   */
  inner class User internal constructor(
    private val client: AirbyteApiClient,
  ) {
    fun getOrCreateUser(): UserRead =
      client.userApi
        .getOrCreateUserByAuthId(UserAuthIdRequestBody(userEmail))
        .userRead
        .also { userIds += it.userId }

    fun getOrgId(workspaceID: UUID): UUID =
      client.workspaceApi.getWorkspace(WorkspaceIdRequestBody(workspaceID, includeTombstone = false)).organizationId

    fun createWorkspace(
      orgId: UUID,
      dataplaneGroupId: UUID? = null,
    ): UUID {
      val workspaceCreateRequest =
        WorkspaceCreate(
          name = "$NAME_PREFIX - ${now()}",
          organizationId = orgId,
          email = userEmail,
          anonymousDataCollection = false,
          news = false,
          securityUpdates = false,
          notifications = emptyList(),
          notificationSettings = NotificationSettings(),
          displaySetupWizard = true,
          dataplaneGroupId = dataplaneGroupId,
          webhookConfigs = emptyList(),
        )

      return client.workspaceApi
        .createWorkspace(workspaceCreateRequest)
        .workspaceId
        .also { workspaceIds += it }
    }
  }

  /**
   * Provides access to admin/internal APIs for managing connectors, connections, and workspace resources.
   * Has full privileges and can perform operations that regular users cannot.
   */
  inner class Admin internal constructor(
    private val client: AirbyteApiClient,
  ) {
    /** helper method for finding the first dataplane group id of an organization */
    fun getDataplaneGroup(orgId: UUID = DEFAULT_ORGANIZATION_ID): UUID =
      adminApiClient.dataplaneGroupApi
        .listDataplaneGroups(DataplaneGroupListRequestBody(orgId))
        .dataplaneGroups
        ?.firstOrNull()
        ?.dataplaneGroupId ?: throw Exception("No Dataplane Group found")

    /** helper method for creating a source */
    fun createSource(
      srcDefId: UUID,
      cfg: String,
    ): UUID = adminApiClient.createSource(workspaceId, srcDefId, cfg).also { srcIds += it }

    /** helper method for creating an acceptance test connector source */
    fun createAtcSource(cfg: AtcConfig = AtcConfig()): UUID {
      val srcDefId = adminApiClient.createSourceDefinition(workspaceId).also { srcDefIds += it }
      return adminApiClient.createAtcSource(workspaceId, srcDefId, cfg).also { srcIds += it }
    }

    /** helper method for updating an acceptance test connector source */
    fun updateAtcSource(
      sourceId: UUID,
      cfg: AtcConfig = AtcConfig(),
    ): UUID = adminApiClient.updateAtcSource(sourceId, cfg)

    /** helper method for getting a source */
    fun getSource(sourceId: UUID): SourceRead = adminApiClient.sourceApi.getSource(SourceIdRequestBody(sourceId))

    /** helper method for getting a source definition */
    fun getSourceDefinition(sourceDefinitionId: UUID): SourceDefinitionRead =
      adminApiClient.sourceDefinitionApi.getSourceDefinition(SourceDefinitionIdRequestBody(sourceDefinitionId))

    /** helper method for getting a source definition spec */
    fun getSourceDefinitionSpec(sourceDefinitionId: UUID): SourceDefinitionSpecificationRead =
      adminApiClient.sourceDefinitionSpecificationApi.getSourceDefinitionSpecification(
        SourceDefinitionIdWithWorkspaceId(sourceDefinitionId, workspaceId),
      )

    /** helper method for creating an acceptance test connector destination */
    fun createAtcDestination(cfg: AtcConfig = AtcConfig()): UUID {
      val dstDefId = adminApiClient.createDestinationDefinition(workspaceId).also { dstDefIds += it }
      return adminApiClient.createAtcDestination(workspaceId, dstDefId, cfg).also { dstIds += it }
    }

    fun updateAtcDestination(
      destinationId: UUID,
      cfg: AtcConfig,
    ): UUID = adminApiClient.updateAtcDestination(destinationId, cfg)

    /**
     * Creates an acceptance test connector connection by setting up a source, a destination, and their corresponding catalog.
     *
     * @param srcCfg Configuration for the source connector. Defaults to a new instance of [AtcConfig].
     * @param dstCfg Configuration for the destination connector. Defaults to a new instance of [AtcConfig].
     * @param modifySrcCatalog A lambda function to modify the discovered source catalog. Defaults to the identity function.
     * @return A [UUID] representing the created connection.
     */
    fun createAtcConnection(
      srcCfg: AtcConfig = AtcConfig(),
      dstCfg: AtcConfig = AtcConfig(),
      dataplaneGroupId: UUID? = null,
      modifySrcCatalog: (AirbyteCatalog) -> AirbyteCatalog = { it },
    ): UUID =
      runBlocking {
        coroutineScope {
          val sourceIdDeferred = async(Dispatchers.IO) { admin.createAtcSource(srcCfg) }
          val destinationIdDeferred = async(Dispatchers.IO) { admin.createAtcDestination(dstCfg) }
          val sourceCatalogDeferred = async(Dispatchers.IO) { discoverSource(sourceIdDeferred.await()) }

          val sourceId = sourceIdDeferred.await()
          val srcCatalog = sourceCatalogDeferred.await()
          val destinationId = destinationIdDeferred.await()

          val catalogId = srcCatalog.catalogId ?: throw IllegalStateException("catalogId is null")
          val catalog = srcCatalog.catalog?.let { modifySrcCatalog(it) } ?: throw IllegalStateException("missing catalog")
          catalog.streams.first().config

          admin.createConnection(
            srcId = sourceId,
            dstId = destinationId,
            catalogId = catalogId,
            catalog = catalog,
            dataplaneGroupId = dataplaneGroupId,
          )
        }
      }

    /** helper method for creating a connection */
    fun createConnection(
      srcId: UUID,
      dstId: UUID,
      catalogId: UUID,
      catalog: AirbyteCatalog,
      dataplaneGroupId: UUID? = null,
    ): UUID {
      val created = now()
      val req =
        ConnectionCreate(
          sourceId = srcId,
          destinationId = dstId,
          status = ConnectionStatus.ACTIVE,
          name = "$NAME_PREFIX - $created",
          namespaceDefinition = NamespaceDefinitionType.SOURCE,
          dataplaneGroupId = dataplaneGroupId,
          syncCatalog = catalog,
          scheduleType = ConnectionScheduleType.MANUAL,
          sourceCatalogId = catalogId,
        )

      return admin.createConnection(req)
    }

    /** helper method for getting all of the active connections */
    fun getActiveConnections(): List<UUID> =
      client.connectionApi
        .listAllConnectionsForWorkspace(WorkspaceIdRequestBody(workspaceId = workspaceId, includeTombstone = false))
        .connections
        .filter { it.status == ConnectionStatus.ACTIVE }
        .map { it.connectionId }

    /** helper method for creating a connection */
    fun createConnection(req: ConnectionCreate): UUID =
      client
        .createConnection(req)
        .also { conIds += it }

    /** helper method for getting a connection */
    fun getConnection(connectionId: UUID): ConnectionRead = adminApiClient.connectionApi.getConnection(ConnectionIdRequestBody(connectionId))

    /** helper method for updating a connection */
    fun updateConnection(request: ConnectionUpdate): UUID = client.connectionApi.updateConnection(connectionUpdate = request).connectionId

    /** helper method for getting a web backend connection with a refreshed catalog */
    fun getWebBackendConnection(connectionId: UUID): WebBackendConnectionRead =
      client.webBackendApi.webBackendGetConnection(
        WebBackendConnectionRequestBody(
          connectionId = connectionId,
          withRefreshedCatalog = true,
        ),
      )

    /** helper method for getting a connection status */
    fun getConnectionStatus(connectionId: UUID): ConnectionStatus = adminApiClient.connectionStatus(connectionId)

    /** helper method for deleting a connection */
    fun deleteConnection(connectionId: UUID) =
      adminApiClient
        .deleteConnection(connectionId)
        .also { conIds -= connectionId }

    /** helper method for syncing a connection */
    fun syncConnection(conId: UUID): Long = adminApiClient.syncConnection(conId)

    /** helper method for running a check on a source */
    fun checkSource(srcId: UUID): CheckConnectionRead = adminApiClient.sourceApi.checkConnectionToSource(SourceIdRequestBody(srcId))

    /** helper method for running a discover on a source */
    fun discoverSource(srcId: UUID): SourceDiscoverSchemaRead =
      adminApiClient.sourceApi.discoverSchemaForSource(SourceDiscoverSchemaRequestBody(sourceId = srcId)).takeIf { it.catalog != null }
        ?: throw IllegalStateException("source $srcId has not catalog")

    /** helper method for running a check on a destination */
    fun checkDestination(dstId: UUID): CheckConnectionRead =
      adminApiClient.destinationApi.checkConnectionToDestination(DestinationIdRequestBody(dstId))

    /** helper method for running a discover on a destination */
    fun discoverDestination(dstId: UUID): DestinationCatalog =
      adminApiClient.destinationApi.discoverCatalogForDestination(DestinationDiscoverSchemaRequestBody(destinationId = dstId)).catalog

    /** blocks until either a job enters a terminal state or the duration is met */
    fun jobWatchUntilTerminal(
      id: Long,
      duration: Duration = 2.minutes,
    ): JobStatus? {
      val start = TimeSource.Monotonic.markNow()

      var status: JobStatus? = null
      while (start.elapsedNow() < duration) {
        status =
          adminApiClient.jobsApi
            .getJobInfoLight(JobIdRequestBody(id))
            .job.status

        when (status) {
          JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED -> break
          else -> Thread.sleep(10.seconds.inWholeMilliseconds)
        }
      }

      return status
    }

    /** helper method for fetching the logs for a job, will write the logs to the provided [log] */
    fun jobLogs(
      jobId: Long,
      log: KLogger,
    ) {
      adminApiClient.jobLogs(jobId).forEach { (attempt, logs) ->
        log.info { "\n\nAttempt: $attempt:\n$logs" }
      }
    }

    /** helper method for cancelling a job */
    fun jobCancel(jobId: Long) = adminApiClient.jobCancel(jobId)

    fun jobMostRecent(connectionId: UUID): Long =
      adminApiClient.jobsApi
        .listJobsFor(JobListRequestBody(configTypes = listOf(JobConfigType.SYNC), configId = connectionId.toString()))
        .jobs
        .firstOrNull()
        ?.job
        ?.id
        ?: throw Exception("No job found for connection $connectionId")

    fun createConnectorBuilderProject(): UUID = client.createConnectorBuilderProject(workspaceId).also { conBuilderIds += it }

    fun publishConnectorBuilderProject(
      projectId: UUID,
      manifest: String,
      spec: String,
    ): UUID =
      client
        .publishConnectorBuilderProject(
          workspaceId = workspaceId,
          builderProjectId = projectId,
          manifest = manifest,
          spec = spec,
        ).also { srcDefIds += it }
  }

  /**
   * Provides access to the public API endpoints that external users would typically use.
   * Includes operations for connector definitions, workspaces, and declarative source definitions.
   */
  inner class Public internal constructor(
    private val client: PublicApiClient,
  ) {
    fun fetchSourceDefinitions(): DefinitionsResponse = client.sourceDefinitionsApi.publicListSourceDefinitions(workspaceId = workspaceId)

    fun fetchSourceDefinition(sourceDefinitionId: UUID): DefinitionResponse =
      client.sourceDefinitionsApi.publicGetSourceDefinition(workspaceId, sourceDefinitionId)

    fun createSourceDefinition(): UUID = client.publicCreateSourceDefinition(workspaceId).also { pubSrcDefIds += it }

    fun updateSourceDefinition(srcDefId: UUID): UUID = client.publicUpdateSourceDefinition(workspaceId, srcDefId)

    fun deleteSourceDefinition(srcDefId: UUID): UUID = client.publicDeleteSourceDefinition(workspaceId, srcDefId).also { pubSrcDefIds -= it }

    fun fetchDestinationDefinition(destinationDefinitionId: UUID): DefinitionResponse =
      client.destinationDefinitionsApi.publicGetDestinationDefinition(workspaceId, destinationDefinitionId)

    fun createDestinationDefinition(): UUID = client.publicCreateDestinationDefinition(workspaceId).also { pubDstDefIds += it }

    fun updateDestinationDefinition(dstDefId: UUID): UUID = client.publicUpdateDestinationDefinition(workspaceId, dstDefId)

    fun deleteDestinationDefinition(dstDefId: UUID): UUID =
      client.publicDeleteDestinationDefinition(workspaceId, dstDefId).also { pubDstDefIds -= it }

    fun createDeclarativeSourceDefinition(manifest: String): UUID =
      client
        .publicCreateDeclarativeSourceDefinition(
          workspaceId = workspaceId,
          name = "$NAME_PREFIX - ${now()}",
          manifest = manifest,
        ).also { pubDecSrcDefIds += it }

    fun updateDeclarativeSourceDefinition(
      decSrcDefId: UUID,
      manifest: String,
    ): UUID =
      client
        .publicUpdateDeclarativeSourceDefinition(
          workspaceId = workspaceId,
          decSrcDefId = decSrcDefId,
          manifest = manifest,
        )

    fun fetchDeclarativeSourceDefinition(decSrcDefId: UUID): DeclarativeSourceDefinitionResponse =
      client.declarativeSourceDefinitionsApi
        .publicGetDeclarativeSourceDefinition(workspaceId, decSrcDefId)

    fun fetchWorkspace(workspaceId: UUID = this@AcceptanceTestClient.workspaceId): WorkspaceResponse =
      client.workspaceApi.publicGetWorkspace(workspaceId.toString())

    fun createWorkspace(request: WorkspaceCreateRequest): UUID =
      client.workspaceApi
        .publicCreateWorkspace(request)
        .let { UUID.fromString(it.workspaceId) }
        .also { workspaceIds += it }

    fun updateWorkspace(
      workspaceId: UUID = this@AcceptanceTestClient.workspaceId,
      request: WorkspaceUpdateRequest,
    ): UUID =
      client.workspaceApi
        .publicUpdateWorkspace(workspaceId.toString(), request)
        .let { UUID.fromString(it.workspaceId) }
  }

  fun setup(workspaceId: UUID) {
    setup(workspaceId.toString())
  }

  fun setup(workspaceId: String? = System.getenv("AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID")) {
    this.workspaceId =
      when {
        !workspaceId.isNullOrBlank() -> {
          log.info { "using workspace id: $workspaceId" }
          UUID.fromString(workspaceId)
        }
        else -> {
          workspaceCreated = true
          createWorkspace().also { log.info { "created new workspace: $it" } }
        }
      }
  }

  fun tearDown() {
    conIds.ensureDelete("connection") {
      adminApiClient.connectionApi.deleteConnection(ConnectionIdRequestBody(it))
    }

    srcIds.ensureDelete("source") {
      adminApiClient.sourceApi.deleteSource(SourceIdRequestBody(it))
    }

    dstIds.ensureDelete("destination") {
      adminApiClient.destinationApi.deleteDestination(DestinationIdRequestBody(it))
    }

    srcDefIds.ensureDelete("source definition") {
      adminApiClient.sourceDefinitionApi.deleteSourceDefinition(SourceDefinitionIdRequestBody(it))
    }

    dstDefIds.ensureDelete("destination definition") {
      adminApiClient.destinationDefinitionApi.deleteDestinationDefinition(DestinationDefinitionIdRequestBody(it))
    }

    pubSrcDefIds.ensureDelete("public source definition") {
      publicApiClient.sourceDefinitionsApi.publicDeleteSourceDefinition(workspaceId, it)
    }

    pubDstDefIds.ensureDelete("public destination definition") {
      publicApiClient.destinationDefinitionsApi.publicDeleteDestinationDefinition(workspaceId, it)
    }

    pubDecSrcDefIds.ensureDelete("public declarative source definition") {
      publicApiClient.declarativeSourceDefinitionsApi.publicDeleteDeclarativeSourceDefinition(workspaceId, it)
    }

    conBuilderIds.ensureDelete("connection builder project") {
      adminApiClient.connectorBuilderProjectApi.deleteConnectorBuilderProject(
        ConnectorBuilderProjectIdWithWorkspaceId(
          workspaceId = workspaceId,
          builderProjectId = it,
        ),
      )
    }
  }

  fun tearDownAll() {
    workspaceIds.ensureDelete("workspace") {
      adminApiClient.deleteWorkspace(it)
    }

    if (workspaceCreated) {
      runCatching { adminApiClient.deleteWorkspace(workspaceId) }
        .onFailure { log.error(it) { "Failed to remove workspace $workspaceId" } }
    }

    userIds.ensureDelete("user") {
      adminApiClient.userApi.deleteUser(UserIdRequestBody(it))
    }
  }
}

/**
 * Call block] for each element in the list, and removes each element from the list regardless of [block] succeeding or not.
 */
private fun MutableSet<UUID>.ensureDelete(
  name: String,
  block: (UUID) -> Unit,
) {
  val iter = this.iterator()
  while (iter.hasNext()) {
    val id = iter.next()
    runCatching { block(id) }.onFailure { log.error(it) { "Failed to remove $name $id" } }
    iter.remove()
  }
}

private fun now(): String {
  val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  return LocalDateTime.now().format(formatter)
}
