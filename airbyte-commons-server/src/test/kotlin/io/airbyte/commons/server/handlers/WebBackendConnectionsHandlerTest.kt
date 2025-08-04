/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.ActorDefinitionVersionRead
import io.airbyte.api.model.generated.ActorStatus
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStream
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AttemptRead
import io.airbyte.api.model.generated.AttemptStatus
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionSchedule
import io.airbyte.api.model.generated.ConnectionState
import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.FieldAdd
import io.airbyte.api.model.generated.FieldRemove
import io.airbyte.api.model.generated.FieldTransform
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.JobWithAttemptsRead
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.api.model.generated.OperationRead
import io.airbyte.api.model.generated.OperationReadList
import io.airbyte.api.model.generated.ResourceRequirements
import io.airbyte.api.model.generated.SchemaChange
import io.airbyte.api.model.generated.SelectedFieldInfo
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.api.model.generated.StreamTransformUpdateStream
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.api.model.generated.Tag
import io.airbyte.api.model.generated.WebBackendConnectionCreate
import io.airbyte.api.model.generated.WebBackendConnectionListFilters
import io.airbyte.api.model.generated.WebBackendConnectionListFilters.StatusesEnum
import io.airbyte.api.model.generated.WebBackendConnectionListItem
import io.airbyte.api.model.generated.WebBackendConnectionListRequestBody
import io.airbyte.api.model.generated.WebBackendConnectionListSortKey
import io.airbyte.api.model.generated.WebBackendConnectionRead
import io.airbyte.api.model.generated.WebBackendConnectionRequestBody
import io.airbyte.api.model.generated.WebBackendConnectionUpdate
import io.airbyte.api.model.generated.WebBackendOperationCreateOrUpdate
import io.airbyte.api.model.generated.WebBackendWorkspaceState
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.ConfigurationUpdate
import io.airbyte.commons.server.handlers.ActorDefinitionVersionHandler
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler.Companion.getSchemaChange
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler.Companion.getStreamsToReset
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler.Companion.toConnectionCreate
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler.Companion.toConnectionPatch
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler.Companion.toOperationUpdate
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.helpers.CatalogConfigDiffHelper
import io.airbyte.commons.server.helpers.ConnectionHelpers
import io.airbyte.commons.server.helpers.ConnectionHelpers.SECOND_FIELD_NAME
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateApiCatalogWithTwoFields
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateBasicApiCatalog
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateBasicConfiguredAirbyteCatalog
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateMultipleStreamsApiCatalog
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateSyncWithSourceAndDestinationId
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateSyncWithSourceId
import io.airbyte.commons.server.helpers.DestinationHelpers
import io.airbyte.commons.server.helpers.DestinationHelpers.generateDestination
import io.airbyte.commons.server.helpers.SourceHelpers
import io.airbyte.commons.server.helpers.SourceHelpers.generateSource
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.commons.temporal.ManualOperationResult
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorCatalogFetchEvent
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.Configs
import io.airbyte.config.JobStatusSummary
import io.airbyte.config.RefreshStream
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ConnectionWithJobInfo
import io.airbyte.data.services.shared.DestinationAndDefinition
import io.airbyte.data.services.shared.SortKey
import io.airbyte.data.services.shared.SortKeyInfo
import io.airbyte.data.services.shared.SourceAndDefinition
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination
import io.airbyte.data.services.shared.buildFilters
import io.airbyte.data.services.shared.parseSortKey
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.airbyte.domain.services.entitlements.ConnectorConfigEntitlementService
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.mappers.transformations.DestinationCatalogGenerator.CatalogGenerationResult
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.Field
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import io.mockk.verifyOrder
import jakarta.validation.Valid
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.io.IOException
import java.lang.reflect.Method
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Arrays
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Collectors

internal class WebBackendConnectionsHandlerTest {
  private lateinit var actorDefinitionVersionHandler: ActorDefinitionVersionHandler
  private lateinit var connectionsHandler: ConnectionsHandler
  private lateinit var operationsHandler: OperationsHandler
  private lateinit var schedulerHandler: SchedulerHandler
  private lateinit var stateHandler: StateHandler
  private lateinit var sourceHandler: SourceHandler
  private lateinit var destinationHandler: DestinationHandler
  private lateinit var wbHandler: WebBackendConnectionsHandler
  private lateinit var sourceRead: SourceRead
  private lateinit var connectionRead: ConnectionRead
  private lateinit var brokenConnectionRead: ConnectionRead
  private lateinit var expectedListItem: WebBackendConnectionListItem
  private lateinit var operationReadList: OperationReadList
  private lateinit var brokenOperationReadList: OperationReadList
  private lateinit var expected: WebBackendConnectionRead
  private lateinit var expectedWithNewSchema: WebBackendConnectionRead
  private lateinit var expectedWithNewSchemaAndBreakingChange: WebBackendConnectionRead
  private lateinit var expectedWithNewSchemaBroken: WebBackendConnectionRead
  private lateinit var expectedNoDiscoveryWithNewSchema: WebBackendConnectionRead
  private lateinit var eventRunner: EventRunner
  private lateinit var catalogService: CatalogService
  private lateinit var connectionService: ConnectionService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper
  private lateinit var destinationCatalogGenerator: DestinationCatalogGenerator
  private lateinit var licenseEntitlementChecker: LicenseEntitlementChecker
  private lateinit var connectorConfigEntitlementService: ConnectorConfigEntitlementService
  private val fieldGenerator = FieldGenerator()
  private val catalogConverter = CatalogConverter(FieldGenerator(), mutableListOf())
  private val applySchemaChangeHelper = ApplySchemaChangeHelper(catalogConverter)
  private val apiPojoConverters = ApiPojoConverters(catalogConverter)
  private lateinit var connectionTimelineEventHelper: ConnectionTimelineEventHelper
  private lateinit var catalogConfigDiffHelper: CatalogConfigDiffHelper
  private lateinit var partialUserConfigService: PartialUserConfigService

  @BeforeEach
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun setup() {
    actorDefinitionVersionHandler = mockk()
    connectionsHandler = mockk(relaxed = true)
    stateHandler = mockk(relaxed = true)
    operationsHandler = mockk(relaxed = true)
    val jobHistoryHandler: JobHistoryHandler = mockk(relaxed = true)
    catalogService = mockk(relaxed = true)
    connectionService = mockk(relaxed = true)
    workspaceService = mockk(relaxed = true)
    workspaceHelper = mockk(relaxed = true)
    schedulerHandler = mockk(relaxed = true)
    eventRunner = mockk(relaxed = true)
    actorDefinitionVersionHelper = mockk(relaxed = true)
    actorDefinitionHandlerHelper = mockk(relaxed = true)
    destinationCatalogGenerator = mockk(relaxed = true)
    connectionTimelineEventHelper = mockk(relaxed = true)
    catalogConfigDiffHelper = mockk(relaxed = true)
    licenseEntitlementChecker = mockk(relaxed = true)
    connectorConfigEntitlementService = mockk(relaxed = true)
    partialUserConfigService = mockk(relaxed = true)

    val validator: JsonSchemaValidator = mockk(relaxed = true)
    val secretsProcessor: JsonSecretsProcessor = mockk(relaxed = true)
    val configurationUpdate: ConfigurationUpdate = mockk(relaxed = true)
    val oAuthConfigSupplier: OAuthConfigSupplier = mockk(relaxed = true)
    val destinationService: DestinationService = mockk(relaxed = true)
    val actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater = mockk(relaxed = true)

    val secretsRepositoryReader: SecretsRepositoryReader = mockk(relaxed = true)
    val sourceService: SourceService = mockk(relaxed = true)
    val secretPersistenceService: SecretPersistenceService = mockk(relaxed = true)
    val secretsRepositoryWriter: SecretsRepositoryWriter = mockk(relaxed = true)
    val secretStorageService: SecretStorageService = mockk(relaxed = true)
    val currentUserService: CurrentUserService = mockk(relaxed = true)

    val secretReferenceService: SecretReferenceService = mockk(relaxed = true)
    every { secretReferenceService.getConfigWithSecretReferences(any(), any(), any()) } answers {
      ConfigWithSecretReferences(secondArg(), mapOf())
    }

    val uuidGenerator: Supplier<UUID> = mockk()

    destinationHandler =
      DestinationHandler(
        validator,
        connectionsHandler,
        uuidGenerator,
        secretsProcessor,
        configurationUpdate,
        oAuthConfigSupplier,
        actorDefinitionVersionHelper,
        destinationService,
        actorDefinitionHandlerHelper,
        actorDefinitionVersionUpdater,
        apiPojoConverters,
        workspaceHelper,
        licenseEntitlementChecker,
        Configs.AirbyteEdition.COMMUNITY,
        secretsRepositoryWriter,
        secretPersistenceService,
        secretStorageService,
        secretReferenceService,
        currentUserService,
        connectorConfigEntitlementService,
      )

    sourceHandler =
      SourceHandler(
        catalogService,
        secretsRepositoryReader,
        validator,
        connectionsHandler,
        uuidGenerator,
        secretsProcessor,
        configurationUpdate,
        oAuthConfigSupplier,
        actorDefinitionVersionHelper,
        sourceService,
        workspaceHelper,
        secretPersistenceService,
        actorDefinitionHandlerHelper,
        actorDefinitionVersionUpdater,
        licenseEntitlementChecker,
        connectorConfigEntitlementService,
        catalogConverter,
        apiPojoConverters,
        Configs.AirbyteEdition.COMMUNITY,
        secretsRepositoryWriter,
        secretStorageService,
        secretReferenceService,
        currentUserService,
        partialUserConfigService,
      )

    wbHandler =
      spyk(
        WebBackendConnectionsHandler(
          actorDefinitionVersionHandler,
          connectionsHandler,
          stateHandler,
          sourceHandler,
          destinationHandler,
          jobHistoryHandler,
          schedulerHandler,
          operationsHandler,
          eventRunner,
          catalogService,
          connectionService,
          actorDefinitionVersionHelper,
          fieldGenerator,
          destinationService,
          sourceService,
          workspaceService,
          catalogConverter,
          applySchemaChangeHelper,
          apiPojoConverters,
          destinationCatalogGenerator,
          connectionTimelineEventHelper,
          catalogConfigDiffHelper,
        ),
      )

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("marketo")
        .withIconUrl(ICON_URL)
    val source = SourceHelpers.generateSource(sourceDefinition.sourceDefinitionId)
    sourceRead = SourceHelpers.getSourceRead(source, sourceDefinition)

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("db2")
        .withIconUrl(ICON_URL)
    val destination = DestinationHelpers.generateDestination(destinationDefinition.destinationDefinitionId)
    val destinationRead = DestinationHelpers.getDestinationRead(destination, destinationDefinition)

    val standardSync =
      ConnectionHelpers.generateSyncWithSourceAndDestinationId(
        source.sourceId,
        destination.destinationId,
        false,
        StandardSync.Status.ACTIVE,
      )
    val brokenStandardSync =
      ConnectionHelpers.generateSyncWithSourceAndDestinationId(source.sourceId, destination.destinationId, true, StandardSync.Status.INACTIVE)
    every { actorDefinitionHandlerHelper.getVersionBreakingChanges(any()) } returns Optional.empty()

    every { connectionService.listWorkspaceStandardSyncs(any()) } returns listOf(standardSync)
    every { connectionService.countWorkspaceStandardSyncs(any(), any()) } returns 1
    every { sourceService.getSourceAndDefinitionsFromSourceIds(listOf(source.sourceId)) } returns
      listOf(
        SourceAndDefinition(
          source,
          sourceDefinition,
        ),
      )
    every { destinationService.getDestinationAndDefinitionsFromDestinationIds(listOf(destination.destinationId)) } returns
      listOf(DestinationAndDefinition(destination, destinationDefinition))

    every { secretsProcessor.prepareSecretsForOutput(source.configuration, any()) } returns source.configuration
    every { secretsProcessor.prepareSecretsForOutput(destination.configuration, any()) } returns destination.configuration

    every { sourceService.getSourceConnection(source.sourceId) } returns source
    every { destinationService.getDestinationConnection(destination.destinationId) } returns destination

    every { sourceService.getSourceDefinitionFromSource(source.sourceId) } returns sourceDefinition
    every { destinationService.getDestinationDefinitionFromDestination(destination.destinationId) } returns destinationDefinition

    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    every { destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId) } returns destinationDefinition

    every { licenseEntitlementChecker.checkEntitlement(any(), any(), any()) } returns true

    val mockSpec = ConnectorSpecification().withConnectionSpecification(Jsons.emptyObject())
    val mockADV = ActorDefinitionVersion().withSpec(mockSpec)
    val mockADVWithOverrideStatus = ActorDefinitionVersionWithOverrideStatus(mockADV, false)
    every { actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(any(), any(), any()) } returns mockADVWithOverrideStatus
    every { actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(any(), any(), any()) } returns mockADVWithOverrideStatus
    every { actorDefinitionVersionHelper.getSourceVersion(any(), any(), any()) } returns mockADV
    every { actorDefinitionVersionHelper.getDestinationVersion(any(), any(), any()) } returns mockADV
    every { actorDefinitionVersionHandler.getActorDefinitionVersionForSourceId(any()) } returns ActorDefinitionVersionRead()
    every { actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(any()) } returns ActorDefinitionVersionRead()

    every { destinationCatalogGenerator.generateDestinationCatalog(any()) } answers {
      CatalogGenerationResult(firstArg(), mapOf())
    }

    connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync)
    brokenConnectionRead = ConnectionHelpers.generateExpectedConnectionRead(brokenStandardSync)

    operationReadList = OperationReadList().operations(listOf(OperationRead().operationId(connectionRead.operationIds[0]).name("Test Operation")))
    brokenOperationReadList =
      OperationReadList().operations(listOf(OperationRead().operationId(brokenConnectionRead.operationIds[0]).name("Test Operation")))

    val now = Instant.now()

// Mock cursor pagination with job info using the fixed timestamp
    every { connectionService.listWorkspaceStandardSyncsCursorPaginated(any(), any()) } returns
      listOf(
        io.airbyte.data.services.shared.ConnectionWithJobInfo(
          standardSync,
          "source",
          "destination",
          Optional.of(io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.succeeded),
          Optional.of(java.time.OffsetDateTime.ofInstant(now, java.time.ZoneOffset.UTC)),
        ),
      )

    val jobRead =
      JobWithAttemptsRead()
        .job(
          JobRead()
            .configId(connectionRead.connectionId.toString())
            .configType(JobConfigType.SYNC)
            .id(10L)
            .status(io.airbyte.api.model.generated.JobStatus.SUCCEEDED)
            .createdAt(now.epochSecond)
            .updatedAt(now.epochSecond),
        ).attempts(
          arrayListOf(
            AttemptRead()
              .id(12L)
              .status(AttemptStatus.SUCCEEDED)
              .bytesSynced(100L)
              .recordsSynced(15L)
              .createdAt(now.epochSecond)
              .updatedAt(now.epochSecond)
              .endedAt(now.epochSecond),
          ),
        )

    every { jobHistoryHandler.getLatestSyncJob(connectionRead.connectionId) } returns Optional.of(jobRead.job)
    every { jobHistoryHandler.getLatestSyncJobsForConnections(listOf(connectionRead.connectionId)) } returns
      listOf(
        JobStatusSummary(
          UUID.fromString(jobRead.job.configId),
          jobRead.job.createdAt,
          io.airbyte.config.JobStatus
            .valueOf(
              jobRead.job.status
                .toString()
                .uppercase(),
            ),
        ),
      )

    val brokenJobRead =
      JobWithAttemptsRead()
        .job(
          JobRead()
            .configId(brokenConnectionRead.connectionId.toString())
            .configType(JobConfigType.SYNC)
            .id(10L)
            .status(io.airbyte.api.model.generated.JobStatus.SUCCEEDED)
            .createdAt(now.epochSecond)
            .updatedAt(now.epochSecond),
        ).attempts(
          arrayListOf(
            AttemptRead()
              .id(12L)
              .status(AttemptStatus.SUCCEEDED)
              .bytesSynced(100L)
              .recordsSynced(15L)
              .createdAt(now.epochSecond)
              .updatedAt(now.epochSecond)
              .endedAt(now.epochSecond),
          ),
        )

    every { jobHistoryHandler.getLatestSyncJob(brokenConnectionRead.connectionId) } returns Optional.of(brokenJobRead.job)
    every { jobHistoryHandler.getLatestSyncJobsForConnections(listOf(brokenConnectionRead.connectionId)) } returns
      listOf(
        JobStatusSummary(
          UUID.fromString(brokenJobRead.job.configId),
          brokenJobRead.job.createdAt,
          io.airbyte.config.JobStatus
            .valueOf(
              brokenJobRead.job.status
                .toString()
                .uppercase(),
            ),
        ),
      )

    expectedListItem =
      ConnectionHelpers.generateExpectedWebBackendConnectionListItem(
        standardSync,
        sourceRead,
        destinationRead,
        false,
        jobRead.job.createdAt,
        jobRead.job.status,
        SchemaChange.NO_CHANGE,
      )

    expected =
      expectedWebBackendConnectionReadObject(
        connectionRead,
        sourceRead,
        destinationRead,
        operationReadList,
        SchemaChange.NO_CHANGE,
        now,
        connectionRead.syncCatalog,
        connectionRead.sourceCatalogId,
      )

    expectedNoDiscoveryWithNewSchema =
      expectedWebBackendConnectionReadObject(
        connectionRead,
        sourceRead,
        destinationRead,
        operationReadList,
        SchemaChange.NON_BREAKING,
        now,
        connectionRead.syncCatalog,
        connectionRead.sourceCatalogId,
      )

    val modifiedCatalog = ConnectionHelpers.generateMultipleStreamsApiCatalog(2)
    val sourceDiscoverSchema =
      SourceDiscoverSchemaRequestBody().apply {
        sourceId = connectionRead.sourceId
        disableCache = true
      }
    every { schedulerHandler.discoverSchemaForSourceFromSourceId(sourceDiscoverSchema) } returns
      SourceDiscoverSchemaRead()
        .jobInfo(mockk())
        .catalog(modifiedCatalog)

    expectedWithNewSchema =
      expectedWebBackendConnectionReadObject(
        connectionRead,
        sourceRead,
        destinationRead,
        OperationReadList().operations(expected.operations),
        SchemaChange.NON_BREAKING,
        now,
        modifiedCatalog,
        null,
      ).catalogDiff(
        CatalogDiff().transforms(
          listOf(
            StreamTransform()
              .transformType(io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum.ADD_STREAM)
              .streamDescriptor(
                io.airbyte.api.model.generated
                  .StreamDescriptor()
                  .name("users-data1"),
              ).updateStream(null),
          ),
        ),
      )

    expectedWithNewSchemaAndBreakingChange =
      expectedWebBackendConnectionReadObject(
        brokenConnectionRead,
        sourceRead,
        destinationRead,
        OperationReadList().operations(expected.operations),
        SchemaChange.BREAKING,
        now,
        modifiedCatalog,
        null,
      ).catalogDiff(
        CatalogDiff().transforms(
          listOf(
            StreamTransform()
              .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
              .streamDescriptor(
                io.airbyte.api.model.generated
                  .StreamDescriptor()
                  .name("users-data1"),
              ).updateStream(null),
          ),
        ),
      )

    expectedWithNewSchemaBroken =
      expectedWebBackendConnectionReadObject(
        brokenConnectionRead,
        sourceRead,
        destinationRead,
        brokenOperationReadList,
        SchemaChange.BREAKING,
        now,
        connectionRead.syncCatalog,
        brokenConnectionRead.sourceCatalogId,
      )

    every { schedulerHandler.resetConnection(any()) } returns JobInfoRead().job(JobRead().status(io.airbyte.api.model.generated.JobStatus.SUCCEEDED))
  }

  fun expectedWebBackendConnectionReadObject(
    connectionRead: ConnectionRead,
    sourceRead: SourceRead?,
    destinationRead: DestinationRead?,
    operationReadList: OperationReadList,
    schemaChange: SchemaChange?,
    now: Instant,
    syncCatalog: AirbyteCatalog?,
    catalogId: UUID?,
  ): WebBackendConnectionRead =
    WebBackendConnectionRead()
      .connectionId(connectionRead.getConnectionId())
      .sourceId(connectionRead.getSourceId())
      .destinationId(connectionRead.getDestinationId())
      .operationIds(connectionRead.getOperationIds())
      .name(connectionRead.getName())
      .namespaceDefinition(connectionRead.getNamespaceDefinition())
      .namespaceFormat(connectionRead.getNamespaceFormat())
      .prefix(connectionRead.getPrefix())
      .syncCatalog(syncCatalog)
      .catalogId(catalogId)
      .status(connectionRead.getStatus())
      .schedule(connectionRead.getSchedule())
      .scheduleType(connectionRead.getScheduleType())
      .scheduleData(connectionRead.getScheduleData())
      .source(sourceRead)
      .destination(destinationRead)
      .operations(operationReadList.getOperations())
      .latestSyncJobCreatedAt(now.getEpochSecond())
      .latestSyncJobStatus(io.airbyte.api.model.generated.JobStatus.SUCCEEDED)
      .isSyncing(false)
      .schemaChange(schemaChange)
      .dataplaneGroupId(connectionRead.getDataplaneGroupId())
      .resourceRequirements(
        ResourceRequirements()
          .cpuRequest(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getCpuRequest())
          .cpuLimit(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getCpuLimit())
          .memoryRequest(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getMemoryRequest())
          .memoryLimit(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.getMemoryLimit()),
      ).notifySchemaChanges(false)
      .notifySchemaChangesByEmail(true)
      .sourceActorDefinitionVersion(ActorDefinitionVersionRead())
      .destinationActorDefinitionVersion(ActorDefinitionVersionRead())

  @Test
  @Throws(IOException::class)
  fun testGetWorkspaceState() {
    val uuid = UUID.randomUUID()
    val request = WebBackendWorkspaceState().workspaceId(uuid)
    every { workspaceService.countSourcesForWorkspace(uuid) } returns 5
    every { workspaceService.countDestinationsForWorkspace(uuid) } returns 2
    every { workspaceService.countConnectionsForWorkspace(uuid) } returns 8
    val actual = wbHandler.getWorkspaceState(request)
    Assertions.assertTrue(actual.getHasConnections())
    Assertions.assertTrue(actual.getHasDestinations())
    Assertions.assertTrue((actual.getHasSources()))
  }

  @Test
  @Throws(IOException::class)
  fun testGetWorkspaceStateEmpty() {
    val uuid = UUID.randomUUID()
    val request = WebBackendWorkspaceState().workspaceId(uuid)
    every { workspaceService.countSourcesForWorkspace(uuid) } returns 0
    every { workspaceService.countDestinationsForWorkspace(uuid) } returns 0
    every { workspaceService.countConnectionsForWorkspace(uuid) } returns 0
    val actual = wbHandler.getWorkspaceState(request)
    Assertions.assertFalse(actual.getHasConnections())
    Assertions.assertFalse(actual.getHasDestinations())
    Assertions.assertFalse(actual.getHasSources())
  }

  @Test
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testWebBackendListConnectionsForWorkspace() {
    val requestBody =
      WebBackendConnectionListRequestBody().apply {
        workspaceId = sourceRead.workspaceId
      }

    every {
      connectionService.buildCursorPagination(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    } returns WorkspaceResourceCursorPagination(null, 10)

    val result = wbHandler.webBackendListConnectionsForWorkspace(requestBody)

    Assertions.assertEquals(1, result.connections.size)
    Assertions.assertEquals(expectedListItem, result.connections[0])
    Assertions.assertEquals(expectedListItem.source.icon, ICON_URL)
    Assertions.assertEquals(expectedListItem.destination.icon, ICON_URL)
  }

  @Test
  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testWebBackendGetConnection() {
    val connectionIdRequestBody = ConnectionIdRequestBody()
    connectionIdRequestBody.setConnectionId(connectionRead.getConnectionId())

    val webBackendConnectionRequestBody = WebBackendConnectionRequestBody()
    webBackendConnectionRequestBody.setConnectionId(connectionRead.getConnectionId())

    every { connectionsHandler.getConnection(connectionRead.getConnectionId()) } returns connectionRead
    every { operationsHandler.listOperationsForConnection(connectionIdRequestBody) } returns operationReadList!!

    val webBackendConnectionRead = wbHandler.webBackendGetConnection(webBackendConnectionRequestBody)

    Assertions.assertEquals(expected, webBackendConnectionRead)

    Assertions.assertEquals(expectedListItem.getSource().getIcon(), ICON_URL)
    Assertions.assertEquals(expectedListItem.getDestination().getIcon(), ICON_URL)
  }

  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testWebBackendGetConnection(
    withCatalogRefresh: Boolean,
    connectionRead: ConnectionRead,
    operationReadList: OperationReadList?,
  ): WebBackendConnectionRead {
    val connectionIdRequestBody = ConnectionIdRequestBody()
    connectionIdRequestBody.setConnectionId(connectionRead.getConnectionId())

    val webBackendConnectionIdRequestBody = WebBackendConnectionRequestBody()
    webBackendConnectionIdRequestBody.setConnectionId(connectionRead.getConnectionId())
    if (withCatalogRefresh) {
      webBackendConnectionIdRequestBody.setWithRefreshedCatalog(true)
    }

    every { connectionsHandler.getConnection(connectionRead.getConnectionId()) } returns connectionRead
    every { operationsHandler.listOperationsForConnection(connectionIdRequestBody) } returns operationReadList!!

    return wbHandler.webBackendGetConnection(webBackendConnectionIdRequestBody)
  }

  @Test
  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testWebBackendGetConnectionWithDiscoveryAndNewSchema() {
    val newCatalogId = UUID.randomUUID()
    every { catalogService.getMostRecentActorCatalogFetchEventForSource(any()) } returns
      Optional.of(ActorCatalogFetchEvent().withActorCatalogId(newCatalogId))
    every { catalogService.getActorCatalogById(any()) } returns ActorCatalog().withId(UUID.randomUUID())
    val schemaRead =
      SourceDiscoverSchemaRead()
        .catalogDiff(expectedWithNewSchema.getCatalogDiff())
        .catalog(expectedWithNewSchema.getSyncCatalog())
        .breakingChange(false)
        .connectionStatus(ConnectionStatus.ACTIVE)
    every { schedulerHandler.discoverSchemaForSourceFromSourceId(any()) } returns schemaRead
    every { connectionsHandler.getConnectionAirbyteCatalog(connectionRead.getConnectionId()) } returns Optional.of(connectionRead.getSyncCatalog())

    val result =
      testWebBackendGetConnection(
        true,
        connectionRead,
        operationReadList,
      )
    Assertions.assertEquals(expectedWithNewSchema, result)
  }

  @Test
  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testWebBackendGetConnectionWithDiscoveryAndNewSchemaBreakingChange() {
    val newCatalogId = UUID.randomUUID()
    every { catalogService.getMostRecentActorCatalogFetchEventForSource(any()) } returns
      Optional.of(ActorCatalogFetchEvent().withActorCatalogId(newCatalogId))
    every { catalogService.getActorCatalogById(any()) } returns ActorCatalog().withId(UUID.randomUUID())
    val schemaRead =
      SourceDiscoverSchemaRead()
        .catalogDiff(expectedWithNewSchema.getCatalogDiff())
        .catalog(expectedWithNewSchema.getSyncCatalog())
        .breakingChange(true)
        .connectionStatus(ConnectionStatus.INACTIVE)
    every { schedulerHandler.discoverSchemaForSourceFromSourceId(any()) } returns schemaRead
    every { connectionsHandler.getConnectionAirbyteCatalog(brokenConnectionRead.getConnectionId()) } returns
      Optional.of(connectionRead.getSyncCatalog())

    val result =
      testWebBackendGetConnection(
        true,
        brokenConnectionRead,
        operationReadList,
      )
    Assertions.assertEquals(expectedWithNewSchemaAndBreakingChange, result)
  }

  @Test
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testWebBackendGetConnectionWithDiscoveryMissingCatalogUsedToMakeConfiguredCatalog() {
    val newCatalogId = UUID.randomUUID()
    every { catalogService.getMostRecentActorCatalogFetchEventForSource(any()) } returns
      Optional.of(ActorCatalogFetchEvent().withActorCatalogId(newCatalogId))
    every { catalogService.getActorCatalogById(any()) } returns ActorCatalog().withId(UUID.randomUUID())
    val schemaRead =
      SourceDiscoverSchemaRead()
        .catalogDiff(expectedWithNewSchema.getCatalogDiff())
        .catalog(expectedWithNewSchema.getSyncCatalog())
        .breakingChange(false)
        .connectionStatus(ConnectionStatus.ACTIVE)
    every { schedulerHandler.discoverSchemaForSourceFromSourceId(any()) } returns schemaRead
    every { connectionsHandler.getConnectionAirbyteCatalog(connectionRead.getConnectionId()) } returns Optional.empty<AirbyteCatalog>()

    val result =
      testWebBackendGetConnection(
        true,
        connectionRead,
        operationReadList,
      )
    Assertions.assertEquals(expectedWithNewSchema, result)
  }

  @Test
  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testWebBackendGetConnectionWithDiscoveryAndFieldSelectionAddField() {
    // Mock this because the API uses it to determine whether there was a schema change.
    every { catalogService.getMostRecentActorCatalogFetchEventForSource(any()) } returns
      Optional.of(ActorCatalogFetchEvent().withActorCatalogId(UUID.randomUUID()))

    // Original configured catalog has two fields, and only one of them is selected.
    val originalConfiguredCatalog = generateApiCatalogWithTwoFields()
    originalConfiguredCatalog
      .getStreams()
      .get(0)
      .getConfig()
      .fieldSelectionEnabled(true)
      .selectedFields(
        listOf<@Valid SelectedFieldInfo?>(
          SelectedFieldInfo().addFieldPathItem(
            ConnectionHelpers.FIELD_NAME,
          ),
        ),
      )
    connectionRead.syncCatalog(originalConfiguredCatalog)

    // Original discovered catalog has the same two fields but no selection info because it's a
    // discovered catalog.
    every { connectionsHandler.getConnectionAirbyteCatalog(connectionRead.getConnectionId()) } returns
      Optional.of<AirbyteCatalog>(generateApiCatalogWithTwoFields())

    // Newly-discovered catalog has an extra field. There is no field selection info because it's a
    // discovered catalog.
    val newCatalogToDiscover = generateApiCatalogWithTwoFields()
    val newFieldSchema = deserialize("{\"type\": \"string\"}")
    (
      newCatalogToDiscover
        .getStreams()
        .get(0)
        .getStream()
        .getJsonSchema()
        .findPath("properties") as ObjectNode
    ).putObject("a-new-field")
      .put("type", "string")
    val schemaRead =
      SourceDiscoverSchemaRead()
        .catalogDiff(
          CatalogDiff()
            .addTransformsItem(
              StreamTransform().updateStream(
                StreamTransformUpdateStream().addFieldTransformsItem(
                  FieldTransform()
                    .transformType(
                      FieldTransform.TransformTypeEnum.ADD_FIELD,
                    ).addFieldNameItem("a-new-field")
                    .breaking(false)
                    .addField(FieldAdd().schema(newFieldSchema)),
                ),
              ),
            ),
        ).catalog(newCatalogToDiscover)
        .breakingChange(false)
        .connectionStatus(ConnectionStatus.ACTIVE)
    every { schedulerHandler.discoverSchemaForSourceFromSourceId(any()) } returns schemaRead

    val result =
      testWebBackendGetConnection(
        true,
        connectionRead,
        operationReadList,
      )

    // We expect the discovered catalog with two fields selected: the one that was originally selected,
    // plus the newly-discovered field.
    val expectedNewCatalog = clone(newCatalogToDiscover)
    expectedNewCatalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).selectedFields(
      listOf<@Valid SelectedFieldInfo?>(
        SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME),
        SelectedFieldInfo().addFieldPathItem("a-new-field"),
      ),
    )
    expectedWithNewSchema.catalogDiff(schemaRead.getCatalogDiff()).syncCatalog(expectedNewCatalog)
    Assertions.assertEquals(expectedWithNewSchema, result)
  }

  @Test
  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testWebBackendGetConnectionWithDiscoveryAndFieldSelectionRemoveField() {
    // Mock this because the API uses it to determine whether there was a schema change.
    every { catalogService.getMostRecentActorCatalogFetchEventForSource(any()) } returns
      Optional.of(ActorCatalogFetchEvent().withActorCatalogId(UUID.randomUUID()))

    // Original configured catalog has two fields, and both of them are selected.
    val originalConfiguredCatalog = generateApiCatalogWithTwoFields()
    originalConfiguredCatalog
      .getStreams()
      .get(0)
      .getConfig()
      .fieldSelectionEnabled(true)
      .selectedFields(
        listOf<@Valid SelectedFieldInfo?>(
          SelectedFieldInfo().addFieldPathItem(
            ConnectionHelpers.FIELD_NAME,
          ),
          SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME + "2"),
        ),
      )
    connectionRead.syncCatalog(originalConfiguredCatalog)

    // Original discovered catalog has the same two fields but no selection info because it's a
    // discovered catalog.
    every { connectionsHandler.getConnectionAirbyteCatalog(connectionRead.getConnectionId()) } returns
      Optional.of<AirbyteCatalog>(generateApiCatalogWithTwoFields())

    // Newly-discovered catalog has one of the fields removed. There is no field selection info because
    // it's a
    // discovered catalog.
    val newCatalogToDiscover = generateBasicApiCatalog()
    val removedFieldSchema = deserialize("{\"type\": \"string\"}")
    val schemaRead =
      SourceDiscoverSchemaRead()
        .catalogDiff(
          CatalogDiff().addTransformsItem(
            StreamTransform().updateStream(
              StreamTransformUpdateStream().addFieldTransformsItem(
                FieldTransform()
                  .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD)
                  .addFieldNameItem(ConnectionHelpers.FIELD_NAME + "2")
                  .breaking(false)
                  .removeField(FieldRemove().schema(removedFieldSchema)),
              ),
            ),
          ),
        ).catalog(newCatalogToDiscover)
        .breakingChange(false)
        .connectionStatus(ConnectionStatus.ACTIVE)
    every { schedulerHandler.discoverSchemaForSourceFromSourceId(any()) } returns schemaRead

    val result =
      testWebBackendGetConnection(
        true,
        connectionRead,
        operationReadList,
      )

    // We expect the discovered catalog with two fields selected: the one that was originally selected,
    // plus the newly-discovered field.
    val expectedNewCatalog = clone(newCatalogToDiscover)
    expectedNewCatalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).selectedFields(
      listOf<@Valid SelectedFieldInfo?>(SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME)),
    )
    expectedWithNewSchema.catalogDiff(schemaRead.getCatalogDiff()).syncCatalog(expectedNewCatalog)
    Assertions.assertEquals(expectedWithNewSchema, result)
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testWebBackendGetConnectionNoRefreshCatalog() {
    val result = testWebBackendGetConnection(false, connectionRead, operationReadList)
    verify(exactly = 0) { schedulerHandler.discoverSchemaForSourceFromSourceId(any()) }
    Assertions.assertEquals(expected, result)
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testWebBackendGetConnectionNoDiscoveryWithNewSchema() {
    every { catalogService.getMostRecentActorCatalogFetchEventForSource(any()) } returns
      Optional.of(ActorCatalogFetchEvent().withActorCatalogId(UUID.randomUUID()))
    every { catalogService.getActorCatalogById(any()) } returns ActorCatalog().withId(UUID.randomUUID())
    val result = testWebBackendGetConnection(false, connectionRead, operationReadList)
    Assertions.assertEquals(expectedNoDiscoveryWithNewSchema, result)
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testWebBackendGetConnectionNoDiscoveryWithNewSchemaBreaking() {
    every { connectionsHandler.getConnection(brokenConnectionRead.getConnectionId()) } returns brokenConnectionRead
    every { catalogService.getMostRecentActorCatalogFetchEventForSource(any()) } returns
      Optional.of(ActorCatalogFetchEvent().withActorCatalogId(UUID.randomUUID()))
    every { catalogService.getActorCatalogById(any()) } returns ActorCatalog().withId(UUID.randomUUID())
    val result = testWebBackendGetConnection(false, brokenConnectionRead, brokenOperationReadList)
    Assertions.assertEquals(expectedWithNewSchemaBroken, result)
  }

  @Test
  @Throws(IOException::class)
  fun testToConnectionCreate() {
    val source = generateSource(UUID.randomUUID())
    val standardSync = generateSyncWithSourceId(source.getSourceId())
    val tags = listOf<Tag?>(Tag().name("tag1"), Tag().name("tag2"))

    val catalog = generateBasicApiCatalog()
    catalog
      .getStreams()
      .get(0)
      .getStream()
      .setName("azkaban_users")

    val schedule = ConnectionSchedule().units(1L).timeUnit(ConnectionSchedule.TimeUnitEnum.MINUTES)

    val newSourceId = UUID.randomUUID()
    val newDestinationId = UUID.randomUUID()
    val newOperationId = UUID.randomUUID()
    val sourceCatalogId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    val input =
      WebBackendConnectionCreate()
        .name("testConnectionCreate")
        .namespaceDefinition(
          standardSync.getNamespaceDefinition().convertTo<NamespaceDefinitionType>(),
        ).namespaceFormat(standardSync.getNamespaceFormat())
        .prefix(standardSync.getPrefix())
        .sourceId(newSourceId)
        .destinationId(newDestinationId)
        .operationIds(listOf<UUID?>(newOperationId))
        .status(ConnectionStatus.INACTIVE)
        .schedule(schedule)
        .syncCatalog(catalog)
        .sourceCatalogId(sourceCatalogId)
        .dataplaneGroupId(dataplaneGroupId)
        .nonBreakingChangesPreference(NonBreakingChangesPreference.DISABLE)
        .tags(tags)

    val operationIds = listOf<UUID?>(newOperationId)

    val expected =
      ConnectionCreate()
        .name("testConnectionCreate")
        .namespaceDefinition(
          standardSync.getNamespaceDefinition().convertTo<NamespaceDefinitionType>(),
        ).namespaceFormat(standardSync.getNamespaceFormat())
        .prefix(standardSync.getPrefix())
        .sourceId(newSourceId)
        .destinationId(newDestinationId)
        .operationIds(operationIds)
        .status(ConnectionStatus.INACTIVE)
        .schedule(schedule)
        .syncCatalog(catalog)
        .sourceCatalogId(sourceCatalogId)
        .dataplaneGroupId(dataplaneGroupId)
        .nonBreakingChangesPreference(NonBreakingChangesPreference.DISABLE)
        .tags(tags)

    val actual = toConnectionCreate(input, operationIds)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  @Throws(IOException::class)
  fun testToConnectionPatch() {
    val source = generateSource(UUID.randomUUID())
    val standardSync = generateSyncWithSourceId(source.getSourceId())
    val tags = listOf<Tag?>(Tag().name("tag1"), Tag().name("tag2"))

    val catalog = generateBasicApiCatalog()
    catalog
      .getStreams()
      .get(0)
      .getStream()
      .setName("azkaban_users")

    val schedule = ConnectionSchedule().units(1L).timeUnit(ConnectionSchedule.TimeUnitEnum.MINUTES)

    val newOperationId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    val input =
      WebBackendConnectionUpdate()
        .namespaceDefinition(
          standardSync.getNamespaceDefinition().convertTo<NamespaceDefinitionType>(),
        ).namespaceFormat(standardSync.getNamespaceFormat())
        .prefix(standardSync.getPrefix())
        .connectionId(standardSync.getConnectionId())
        .operations(listOf<@Valid WebBackendOperationCreateOrUpdate?>(WebBackendOperationCreateOrUpdate().operationId(newOperationId)))
        .status(ConnectionStatus.INACTIVE)
        .schedule(schedule)
        .name(standardSync.getName())
        .syncCatalog(catalog)
        .dataplaneGroupId(dataplaneGroupId)
        .nonBreakingChangesPreference(NonBreakingChangesPreference.DISABLE)
        .notifySchemaChanges(false)
        .notifySchemaChangesByEmail(true)
        .tags(tags)

    val operationIds = listOf<UUID?>(newOperationId)

    val expected =
      ConnectionUpdate()
        .namespaceDefinition(
          standardSync.getNamespaceDefinition().convertTo<NamespaceDefinitionType>(),
        ).namespaceFormat(standardSync.getNamespaceFormat())
        .prefix(standardSync.getPrefix())
        .connectionId(standardSync.getConnectionId())
        .operationIds(operationIds)
        .status(ConnectionStatus.INACTIVE)
        .schedule(schedule)
        .name(standardSync.getName())
        .syncCatalog(catalog)
        .dataplaneGroupId(dataplaneGroupId)
        .nonBreakingChangesPreference(NonBreakingChangesPreference.DISABLE)
        .notifySchemaChanges(false)
        .notifySchemaChangesByEmail(true)
        .breakingChange(false)
        .tags(tags)

    val actual = toConnectionPatch(input, operationIds, false)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testForConnectionCreateCompleteness() {
    val handledMethods =
      mutableSetOf<String?>(
        "name",
        "namespaceDefinition",
        "namespaceFormat",
        "prefix",
        "sourceId",
        "destinationId",
        "operationIds",
        "addOperationIdsItem",
        "removeOperationIdsItem",
        "syncCatalog",
        "schedule",
        "scheduleType",
        "scheduleData",
        "status",
        "resourceRequirements",
        "sourceCatalogId",
        "destinationCatalogId",
        "dataplaneGroupId",
        "nonBreakingChangesPreference",
        "notifySchemaChanges",
        "notifySchemaChangesByEmail",
        "backfillPreference",
        "tags",
        "addTagsItem",
        "removeTagsItem",
      )

    val methods =
      Arrays
        .stream(ConnectionCreate::class.java.getMethods())
        .filter { method: Method? -> method!!.getReturnType() == ConnectionCreate::class.java }
        .map { obj: Method? -> obj!!.getName() }
        .collect(Collectors.toSet())

    val message =
      """
      If this test is failing, it means you added a field to ConnectionCreate!
      Congratulations, but you're not done yet..
      ${'\t'}You should update WebBackendConnectionsHandler::toConnectionCreate
      ${'\t'}and ensure that the field is tested in WebBackendConnectionsHandlerTest::testToConnectionCreate
      Then you can add the field name here to make this test pass. Cheers!
      """.trimIndent()
    Assertions.assertEquals(handledMethods, methods, message)
  }

  @Test
  fun testForConnectionPatchCompleteness() {
    val handledMethods =
      mutableSetOf<String?>(
        "schedule",
        "connectionId",
        "syncCatalog",
        "namespaceDefinition",
        "namespaceFormat",
        "prefix",
        "status",
        "operationIds",
        "addOperationIdsItem",
        "removeOperationIdsItem",
        "resourceRequirements",
        "name",
        "sourceCatalogId",
        "destinationCatalogId",
        "scheduleType",
        "scheduleData",
        "dataplaneGroupId",
        "breakingChange",
        "notifySchemaChanges",
        "notifySchemaChangesByEmail",
        "nonBreakingChangesPreference",
        "backfillPreference",
        "tags",
        "addTagsItem",
        "removeTagsItem",
      )

    val methods =
      Arrays
        .stream(ConnectionUpdate::class.java.getMethods())
        .filter { method: Method? -> method!!.getReturnType() == ConnectionUpdate::class.java }
        .map { obj: Method? -> obj!!.getName() }
        .collect(Collectors.toSet())

    val message =
      """
      If this test is failing, it means you added a field to ConnectionUpdate!
      Congratulations, but you're not done yet..
      ${'\t'}You should update WebBackendConnectionsHandler::toConnectionPatch
      ${'\t'}and ensure that the field is tested in WebBackendConnectionsHandlerTest::testToConnectionPatch
      Then you can add the field name here to make this test pass. Cheers!
      """.trimIndent()
    Assertions.assertEquals(handledMethods, methods, message)
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testUpdateConnection() {
    val updateBody =
      WebBackendConnectionUpdate()
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .connectionId(expected.getConnectionId())
        .schedule(expected.getSchedule())
        .status(expected.getStatus())
        .syncCatalog(expected.getSyncCatalog())
        .sourceCatalogId(expected.getCatalogId())
        .notifySchemaChanges(expected.getNotifySchemaChanges())
        .notifySchemaChangesByEmail(expected.getNotifySchemaChangesByEmail())

    every { connectionService.getConfiguredCatalogForConnection(expected.getConnectionId()) } returns generateBasicConfiguredAirbyteCatalog()

    val catalogDiff = CatalogDiff().transforms(mutableListOf<@Valid StreamTransform?>())
    every {
      connectionsHandler.getDiff(
        any(),
        any(),
        any(),
        any(),
      )
    } returns catalogDiff
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(expected.getConnectionId())
    every { stateHandler.getState(connectionIdRequestBody) } returns ConnectionState().stateType(ConnectionStateType.LEGACY)

    every { connectionsHandler.getConnection(expected.getConnectionId()) } returns
      ConnectionRead().connectionId(expected.getConnectionId()).sourceId(expected.getSourceId())
    every {
      connectionsHandler.updateConnection(
        any(),
        any(),
        any<Boolean>(),
      )
    } returns
      ConnectionRead()
        .connectionId(expected.getConnectionId())
        .sourceId(expected.getSourceId())
        .destinationId(expected.getDestinationId())
        .name(expected.getName())
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .syncCatalog(expected.getSyncCatalog())
        .status(expected.getStatus())
        .schedule(expected.getSchedule())
        .breakingChange(false)
        .notifySchemaChanges(expected.getNotifySchemaChanges())
        .notifySchemaChangesByEmail(expected.getNotifySchemaChangesByEmail())

    every { operationsHandler.listOperationsForConnection(any()) } returns operationReadList
    val connectionId = ConnectionIdRequestBody().connectionId(connectionRead.getConnectionId())

    val fullAirbyteCatalog = generateMultipleStreamsApiCatalog(2)
    every { connectionsHandler.getConnectionAirbyteCatalog(connectionRead.getConnectionId()) } returns
      Optional.ofNullable<AirbyteCatalog>(fullAirbyteCatalog)

    val expectedCatalogReturned =
      wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(
        expected.getSyncCatalog(),
        expected.getSyncCatalog(),
        fullAirbyteCatalog,
      )
    val connectionRead = wbHandler.webBackendUpdateConnection(updateBody)

    Assertions.assertEquals(expectedCatalogReturned, connectionRead.getSyncCatalog())

    verify(exactly = 0) { schedulerHandler.resetConnection(connectionId) }
    verify(exactly = 0) { schedulerHandler.syncConnection(connectionId) }
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testUpdateConnectionWithOperations() {
    val operationCreateOrUpdate =
      WebBackendOperationCreateOrUpdate()
        .name("Test Operation")
        .operationId(connectionRead.getOperationIds().get(0))
    val operationUpdate = toOperationUpdate(operationCreateOrUpdate)
    val updateBody =
      WebBackendConnectionUpdate()
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .connectionId(expected.getConnectionId())
        .schedule(expected.getSchedule())
        .status(expected.getStatus())
        .syncCatalog(expected.getSyncCatalog())
        .operations(listOf<@Valid WebBackendOperationCreateOrUpdate?>(operationCreateOrUpdate))

    every { connectionService.getConfiguredCatalogForConnection(expected.getConnectionId()) } returns generateBasicConfiguredAirbyteCatalog()

    val catalogDiff = CatalogDiff().transforms(mutableListOf<@Valid StreamTransform?>())
    every {
      connectionsHandler.getDiff(
        any(),
        any(),
        any(),
        any(),
      )
    } returns catalogDiff
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(expected.getConnectionId())
    every { stateHandler.getState(connectionIdRequestBody) } returns ConnectionState().stateType(ConnectionStateType.LEGACY)

    every { connectionsHandler.getConnection(expected.getConnectionId()) } returns
      ConnectionRead()
        .connectionId(expected.getConnectionId())
        .sourceId(expected.getSourceId())
        .operationIds(connectionRead.getOperationIds())
        .breakingChange(false)

    every { connectionsHandler.updateConnection(any(), any(), any<Boolean>()) } returns
      ConnectionRead()
        .connectionId(expected.getConnectionId())
        .sourceId(expected.getSourceId())
        .destinationId(expected.getDestinationId())
        .operationIds(connectionRead.getOperationIds())
        .name(expected.getName())
        .namespaceDefinition(expected.getNamespaceDefinition())
        .namespaceFormat(expected.getNamespaceFormat())
        .prefix(expected.getPrefix())
        .syncCatalog(expected.getSyncCatalog())
        .status(expected.getStatus())
        .schedule(expected.getSchedule())
        .breakingChange(false)
    every { operationsHandler.updateOperation(operationUpdate) } returns OperationRead().operationId(operationUpdate.getOperationId())
    every { operationsHandler.listOperationsForConnection(any()) } returns operationReadList

    val actualConnectionRead = wbHandler.webBackendUpdateConnection(updateBody)

    Assertions.assertEquals(connectionRead.getOperationIds(), actualConnectionRead.getOperationIds())
    verify(exactly = 1) { operationsHandler.updateOperation(operationUpdate) }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testUpdateConnectionWithUpdatedSchemaPerStream(useRefresh: Boolean) {
    every { actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(any()) } returns
      ActorDefinitionVersionRead().supportsRefreshes(useRefresh)

    val updateBody =
      WebBackendConnectionUpdate()
        .namespaceDefinition(expected.namespaceDefinition)
        .namespaceFormat(expected.namespaceFormat)
        .prefix(expected.prefix)
        .connectionId(expected.connectionId)
        .schedule(expected.schedule)
        .status(expected.status)
        .syncCatalog(expectedWithNewSchema.syncCatalog)

    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(expected.connectionId)
    every { stateHandler.getState(connectionIdRequestBody) } returns ConnectionState().stateType(ConnectionStateType.STREAM)
    every { connectionService.getConfiguredCatalogForConnection(expected.connectionId) } returns
      ConnectionHelpers.generateBasicConfiguredAirbyteCatalog()

    val streamTransformAdd =
      StreamTransform()
        .streamDescriptor(
          io.airbyte.api.model.generated
            .StreamDescriptor()
            .name("addStream"),
        ).transformType(io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum.ADD_STREAM)
    val streamTransformRemove =
      StreamTransform()
        .streamDescriptor(
          io.airbyte.api.model.generated
            .StreamDescriptor()
            .name("removeStream"),
        ).transformType(io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum.REMOVE_STREAM)
    val streamTransformUpdate =
      StreamTransform()
        .streamDescriptor(
          io.airbyte.api.model.generated
            .StreamDescriptor()
            .name("updateStream"),
        ).transformType(io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .updateStream(StreamTransformUpdateStream())

    val catalogDiff = CatalogDiff().transforms(listOf(streamTransformAdd, streamTransformRemove, streamTransformUpdate))
    every { connectionsHandler.getDiff(any(), any(), any(), any()) } returns catalogDiff
    every { connectionsHandler.getConfigurationDiff(any(), any()) } returns
      setOf(
        io.airbyte.api.model.generated
          .StreamDescriptor()
          .name("configUpdateStream"),
      )

    every { operationsHandler.listOperationsForConnection(any()) } returns operationReadList
    every { connectionsHandler.getConnection(expected.connectionId) } returns
      ConnectionRead().connectionId(expected.connectionId).breakingChange(false)

    val connectionRead =
      ConnectionRead()
        .connectionId(expected.connectionId)
        .sourceId(expected.sourceId)
        .destinationId(expected.destinationId)
        .name(expected.name)
        .namespaceDefinition(expected.namespaceDefinition)
        .namespaceFormat(expected.namespaceFormat)
        .prefix(expected.prefix)
        .syncCatalog(expectedWithNewSchema.syncCatalog)
        .status(expected.status)
        .schedule(expected.schedule)
        .breakingChange(false)
    every { connectionsHandler.updateConnection(any(), any(), any()) } returns connectionRead
    every { connectionsHandler.getConnection(expected.connectionId) } returns connectionRead

    val successfulResult = ManualOperationResult(null, null, null)
    every { eventRunner.resetConnection(any(), any()) } returns successfulResult
    every { eventRunner.startNewManualSync(any()) } returns successfulResult

    every { catalogService.getMostRecentActorCatalogForSource(any()) } returns Optional.of(ActorCatalog().withCatalog(Jsons.emptyObject()))

    val result = wbHandler.webBackendUpdateConnection(updateBody)

    Assertions.assertEquals(expectedWithNewSchema.syncCatalog, result.syncCatalog)

    verify { destinationCatalogGenerator.generateDestinationCatalog(catalogConverter.toConfiguredInternal(expectedWithNewSchema.syncCatalog)) }
    verify { destinationCatalogGenerator.generateDestinationCatalog(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog()) }

    val connectionId = ConnectionIdRequestBody().connectionId(result.connectionId)
    verify(exactly = 0) { schedulerHandler.resetConnection(connectionId) }
    verify(exactly = 0) { schedulerHandler.syncConnection(connectionId) }
    verify(exactly = 1) { connectionsHandler.updateConnection(any(), any(), any()) }

    val expectedStreams =
      listOf(
        io.airbyte.config
          .StreamDescriptor()
          .withName("addStream"),
        io.airbyte.config
          .StreamDescriptor()
          .withName("updateStream"),
        io.airbyte.config
          .StreamDescriptor()
          .withName("configUpdateStream"),
        io.airbyte.config
          .StreamDescriptor()
          .withName("removeStream"),
      )

    if (useRefresh) {
      verifyOrder {
        eventRunner.refreshConnectionAsync(connectionId.connectionId, expectedStreams, io.airbyte.config.RefreshStream.RefreshType.MERGE)
      }
    } else {
      verifyOrder {
        eventRunner.resetConnectionAsync(connectionId.connectionId, expectedStreams)
      }
    }
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testUpdateConnectionNoStreamsToReset() {
    val updateBody =
      WebBackendConnectionUpdate()
        .namespaceDefinition(expected.namespaceDefinition)
        .namespaceFormat(expected.namespaceFormat)
        .prefix(expected.prefix)
        .connectionId(expected.connectionId)
        .schedule(expected.schedule)
        .status(expected.status)
        .syncCatalog(expectedWithNewSchema.syncCatalog)

    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(expected.connectionId)
    val configuredCatalog = ConnectionHelpers.generateBasicConfiguredAirbyteCatalog()
    every { stateHandler.getState(connectionIdRequestBody) } returns ConnectionState().stateType(ConnectionStateType.STREAM)
    every { connectionService.getConfiguredCatalogForConnection(expected.connectionId) } returns configuredCatalog
    every { connectionsHandler.getDiff(any(), any(), any(), any()) } returns CatalogDiff().transforms(emptyList())
    every { operationsHandler.listOperationsForConnection(any()) } returns operationReadList
    every { connectionsHandler.getConnection(expected.connectionId) } returns ConnectionRead().connectionId(expected.connectionId)

    val connectionRead =
      ConnectionRead()
        .connectionId(expected.connectionId)
        .sourceId(expected.sourceId)
        .destinationId(expected.destinationId)
        .name(expected.name)
        .namespaceDefinition(expected.namespaceDefinition)
        .namespaceFormat(expected.namespaceFormat)
        .prefix(expected.prefix)
        .syncCatalog(expectedWithNewSchema.syncCatalog)
        .status(expected.status)
        .schedule(expected.schedule)
        .breakingChange(false)
    every { connectionsHandler.updateConnection(any(), any(), any()) } returns connectionRead
    every { connectionsHandler.getConnection(expected.connectionId) } returns connectionRead

    val result = wbHandler.webBackendUpdateConnection(updateBody)

    Assertions.assertEquals(expectedWithNewSchema.syncCatalog, result.syncCatalog)

    val connectionId = ConnectionIdRequestBody().connectionId(result.connectionId)

    verify {
      connectionsHandler.getDiff(
        expected.syncCatalog,
        expectedWithNewSchema.syncCatalog,
        catalogConverter.toConfiguredInternal(result.syncCatalog),
        expected.connectionId,
      )
    }
    verify { connectionsHandler.getConfigurationDiff(expected.syncCatalog, expectedWithNewSchema.syncCatalog) }
    verify(exactly = 0) { schedulerHandler.resetConnection(connectionId) }
    verify(exactly = 0) { schedulerHandler.syncConnection(connectionId) }
    verify(exactly = 1) { connectionsHandler.updateConnection(any(), any(), any()) }
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testUpdateConnectionFixingBreakingSchemaChange() {
    val updateBody =
      WebBackendConnectionUpdate()
        .namespaceDefinition(expected.namespaceDefinition)
        .namespaceFormat(expected.namespaceFormat)
        .prefix(expected.prefix)
        .connectionId(expected.connectionId)
        .schedule(expected.schedule)
        .status(expected.status)
        .syncCatalog(expectedWithNewSchema.syncCatalog)
        .skipReset(false)

    val sourceId = sourceRead.sourceId

    every { connectionsHandler.getConnection(expected.connectionId) } returns
      ConnectionRead()
        .connectionId(expected.connectionId)
        .breakingChange(true)
        .syncCatalog(expectedWithNewSchema.syncCatalog)
        .sourceId(sourceId)

    val catalogJson =
      """
      {
        "streams": [{
          "name": "cat_names",
          "namespace": "public",
          "json_schema": {
            "type": "object",
            "properties": {
              "id": {
                "type": "number",
                "airbyte_type": "integer"
              }
            }
          }
        }]
      }
      """.trimIndent()

    val catalogDiff = CatalogDiff().transforms(emptyList())
    every { catalogService.getMostRecentActorCatalogForSource(sourceId) } returns
      Optional.of(ActorCatalog().withCatalog(Jsons.deserialize(catalogJson)))
    every { connectionsHandler.getDiff(any(), any(), any(), any()) } returns catalogDiff andThen catalogDiff

    every { connectionService.getConfiguredCatalogForConnection(expected.connectionId) } returns
      ConnectionHelpers.generateBasicConfiguredAirbyteCatalog()
    every { operationsHandler.listOperationsForConnection(any()) } returns operationReadList

    val connectionRead =
      ConnectionRead()
        .connectionId(expected.connectionId)
        .sourceId(expected.sourceId)
        .destinationId(expected.destinationId)
        .name(expected.name)
        .namespaceDefinition(expected.namespaceDefinition)
        .namespaceFormat(expected.namespaceFormat)
        .prefix(expected.prefix)
        .syncCatalog(expectedWithNewSchema.syncCatalog)
        .status(expected.status)
        .schedule(expected.schedule)
        .breakingChange(false)
    every { connectionsHandler.updateConnection(any(), any(), any()) } returns connectionRead

    val result = wbHandler.webBackendUpdateConnection(updateBody)

    Assertions.assertEquals(expectedWithNewSchema.syncCatalog, result.syncCatalog)

    val connectionId = ConnectionIdRequestBody().connectionId(result.connectionId)

    val captor = slot<ConnectionUpdate>()
    verify(exactly = 1) { connectionsHandler.updateConnection(capture(captor), any(), any()) }
    Assertions.assertEquals(false, captor.captured.breakingChange)

    verify(exactly = 0) { schedulerHandler.resetConnection(connectionId) }
    verify(exactly = 0) { schedulerHandler.syncConnection(connectionId) }
    verify(exactly = 2) { connectionsHandler.getDiff(any(), any(), any(), any()) }
    verify(exactly = 1) { connectionsHandler.updateConnection(any(), any(), any()) }
  }

  @Test
  fun testUpdateSchemaWithDiscoveryFromEmpty() {
    val original = AirbyteCatalog().streams(mutableListOf<@Valid AirbyteStreamAndConfiguration?>())
    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    discovered
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)

    val expected = generateBasicApiCatalog()
    expected
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    expected
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)
      .selected(false)
      .suggested(false)
      .selectedFields(mutableListOf<@Valid SelectedFieldInfo?>())

    val actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testUpdateSchemaWithDiscoveryResetStream() {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .get(0)
      .getStream()
      .name("random-stream")
      .defaultCursorField(listOf<String?>(FIELD1))
      .jsonSchema(
        CatalogHelpers.fieldsToJsonSchema(
          Field.of(FIELD1, JsonSchemaType.NUMBER),
          Field.of(FIELD2, JsonSchemaType.NUMBER),
          Field.of(FIELD5, JsonSchemaType.STRING),
        ),
      ).supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    original
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.INCREMENTAL)
      .cursorField(listOf<String?>(FIELD1))
      .destinationSyncMode(DestinationSyncMode.APPEND)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName("random_stream")

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .defaultCursorField(listOf<String?>(FIELD3))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    discovered
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)

    val expected = generateBasicApiCatalog()
    expected
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .defaultCursorField(listOf<String?>(FIELD3))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    expected
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)
      .selected(false)
      .suggested(false)
      .selectedFields(mutableListOf<@Valid SelectedFieldInfo?>())

    val actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testUpdateSchemaWithDiscoveryMergeNewStream() {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .defaultCursorField(listOf<String?>(FIELD1))
      .jsonSchema(
        CatalogHelpers.fieldsToJsonSchema(
          Field.of(FIELD1, JsonSchemaType.NUMBER),
          Field.of(FIELD2, JsonSchemaType.NUMBER),
          Field.of(FIELD5, JsonSchemaType.STRING),
        ),
      ).supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    original
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.INCREMENTAL)
      .cursorField(listOf<String?>(FIELD1))
      .destinationSyncMode(DestinationSyncMode.APPEND)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName("renamed_stream")

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .defaultCursorField(listOf<String?>(FIELD3))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    discovered
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)
    val newStream = generateBasicApiCatalog().getStreams().get(0)
    newStream
      .getStream()
      .name(STREAM2)
      .defaultCursorField(listOf<String?>(FIELD5))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD5, JsonSchemaType.BOOLEAN)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    newStream
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM2)
    discovered.getStreams().add(newStream)

    val expected = generateBasicApiCatalog()
    expected
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .defaultCursorField(listOf<String?>(FIELD3))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    expected
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.INCREMENTAL)
      .cursorField(listOf<String?>(FIELD1))
      .destinationSyncMode(DestinationSyncMode.APPEND)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName("renamed_stream")
      .selected(true)
      .suggested(false)
      .selectedFields(mutableListOf<@Valid SelectedFieldInfo?>())
    val expectedNewStream = generateBasicApiCatalog().getStreams().get(0)
    expectedNewStream
      .getStream()
      .name(STREAM2)
      .defaultCursorField(listOf<String?>(FIELD5))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD5, JsonSchemaType.BOOLEAN)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    expectedNewStream
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM2)
      .selected(false)
      .suggested(false)
      .selectedFields(mutableListOf<@Valid SelectedFieldInfo?>())
    expected.getStreams().add(expectedNewStream)

    val actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithChangedSourceDefinedPK() {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .first()
      .getStream()
      .sourceDefinedPrimaryKey(listOf(listOf<String?>(FIELD1)))
    original
      .getStreams()
      .first()
      .getConfig()
      .primaryKey(listOf(listOf<String?>(FIELD1)))

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .first()
      .getStream()
      .sourceDefinedPrimaryKey(listOf(listOf<String?>(FIELD2)))
    discovered
      .getStreams()
      .first()
      .getConfig()
      .primaryKey(listOf(listOf<String?>(FIELD2)))

    val actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered)

    // Use new value for source-defined PK
    Assertions.assertEquals(
      listOf(listOf<String?>(FIELD2)),
      actual
        .getStreams()
        .first()
        .getConfig()
        .getPrimaryKey(),
    )
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithNoSourceDefinedPK() {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .first()
      .getConfig()
      .primaryKey(listOf(listOf<String?>(FIELD1)))

    val discovered = generateBasicApiCatalog()
    Assertions.assertNotEquals(
      original
        .getStreams()
        .first()
        .getConfig()
        .getPrimaryKey(),
      discovered
        .getStreams()
        .first()
        .getConfig()
        .getPrimaryKey(),
    )

    val actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered)

    // Keep previously-configured PK
    Assertions.assertEquals(
      listOf(listOf<String?>(FIELD1)),
      actual
        .getStreams()
        .first()
        .getConfig()
        .getPrimaryKey(),
    )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testUpdateSchemaWithDiscoveryWithIncludeFiles(includeFiles: Boolean) {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .first()
      .getConfig()
      .includeFiles(includeFiles)

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .first()
      .getConfig()
      .includeFiles(false)

    val actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered)

    // Use new value for include files
    Assertions.assertEquals(
      includeFiles,
      actual
        .getStreams()
        .first()
        .getConfig()
        .getIncludeFiles(),
    )
  }

  @Test
  fun testUpdateSchemaWithDestinationObjectName() {
    val configured = generateBasicApiCatalog()
    configured
      .getStreams()
      .first()
      .getConfig()
      .destinationObjectName("configured_object_name")

    val discovered = generateBasicApiCatalog()

    val actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(configured, discovered, discovered)

    Assertions.assertEquals(
      "configured_object_name",
      actual
        .getStreams()
        .first()
        .getConfig()
        .getDestinationObjectName(),
    )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testUpdateSchemaWithDiscoveryWithFileBased(isFileBased: Boolean) {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .first()
      .getStream()
      .isFileBased(false)

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .first()
      .getStream()
      .isFileBased(isFileBased)

    val actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered)

    Assertions.assertEquals(
      isFileBased,
      actual
        .getStreams()
        .first()
        .getStream()
        .getIsFileBased(),
    )
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithHashedField() {
    val hashedFields = listOf<SelectedFieldInfo?>(SelectedFieldInfo().fieldPath(listOf<String?>(SECOND_FIELD_NAME)))

    val original = generateApiCatalogWithTwoFields()
    original
      .getStreams()
      .first()
      .getConfig()
      .setHashedFields(hashedFields)

    val discovered = generateApiCatalogWithTwoFields()

    val actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered)

    // Configure hashed fields
    Assertions.assertEquals(
      hashedFields,
      actual
        .getStreams()
        .first()
        .getConfig()
        .getHashedFields(),
    )
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithRemovedHashedField() {
    val original = generateApiCatalogWithTwoFields()
    original
      .getStreams()
      .first()
      .getConfig()
      .setHashedFields(listOf<@Valid SelectedFieldInfo?>(SelectedFieldInfo().fieldPath(listOf<String?>(SECOND_FIELD_NAME))))

    val discovered = generateBasicApiCatalog()

    val actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered)

    // Remove hashed field
    Assertions.assertTrue(
      actual
        .getStreams()
        .first()
        .getConfig()
        .getHashedFields()
        .isEmpty(),
    )
  }

  @Test
  fun testUpdateSchemaWithNamespacedStreams() {
    val original = generateBasicApiCatalog()
    val stream1Config = original.getStreams().get(0)
    val stream1 = stream1Config.getStream()
    val stream2 =
      AirbyteStream()
        .name(stream1.getName())
        .namespace("second_namespace")
        .jsonSchema(stream1.getJsonSchema())
        .defaultCursorField(stream1.getDefaultCursorField())
        .supportedSyncModes(stream1.getSupportedSyncModes())
        .sourceDefinedCursor(stream1.getSourceDefinedCursor())
        .sourceDefinedPrimaryKey(stream1.getSourceDefinedPrimaryKey())
    val stream2Config =
      AirbyteStreamAndConfiguration()
        .config(stream1Config.getConfig())
        .stream(stream2)
    original.getStreams().add(stream2Config)

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    discovered
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)

    val expected = generateBasicApiCatalog()
    expected
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    expected
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)
      .selected(false)
      .suggested(false)
      .selectedFields(mutableListOf<@Valid SelectedFieldInfo?>())

    val actual = wbHandler.updateSchemaWithRefreshedDiscoveredCatalog(original, original, discovered)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testGetStreamsToReset() {
    val streamTransformAdd =
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(
          io.airbyte.api.model.generated
            .StreamDescriptor()
            .name("added_stream"),
        )
    val streamTransformRemove =
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(
          io.airbyte.api.model.generated
            .StreamDescriptor()
            .name("removed_stream"),
        )
    val streamTransformUpdate =
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(
          io.airbyte.api.model.generated
            .StreamDescriptor()
            .name("updated_stream"),
        )
    val catalogDiff =
      CatalogDiff().transforms(listOf<@Valid StreamTransform?>(streamTransformAdd, streamTransformRemove, streamTransformUpdate))
    val resultList: List<io.airbyte.api.model.generated.StreamDescriptor> = getStreamsToReset(catalogDiff)
    Assertions.assertTrue(
      resultList.stream().anyMatch { streamDescriptor: io.airbyte.api.model.generated.StreamDescriptor? ->
        "added_stream".equals(
          streamDescriptor!!.getName(),
          ignoreCase = true,
        )
      },
    )
    Assertions.assertTrue(
      resultList.stream().anyMatch { streamDescriptor: io.airbyte.api.model.generated.StreamDescriptor? ->
        "removed_stream".equals(
          streamDescriptor!!.getName(),
          ignoreCase = true,
        )
      },
    )
    Assertions.assertTrue(
      resultList.stream().anyMatch { streamDescriptor: io.airbyte.api.model.generated.StreamDescriptor? ->
        "updated_stream".equals(
          streamDescriptor!!.getName(),
          ignoreCase = true,
        )
      },
    )
  }

  @Test
  fun testGetSchemaChangeNoChange() {
    val connectionReadNotBreaking = ConnectionRead().breakingChange(false)

    Assertions.assertEquals(
      SchemaChange.NO_CHANGE,
      getSchemaChange(null, Optional.of(UUID.randomUUID()), Optional.of<ActorCatalogFetchEvent>(ActorCatalogFetchEvent())),
    )
    Assertions.assertEquals(
      SchemaChange.NO_CHANGE,
      getSchemaChange(connectionReadNotBreaking, Optional.empty<UUID>(), Optional.of<ActorCatalogFetchEvent>(ActorCatalogFetchEvent())),
    )

    val catalogId = UUID.randomUUID()

    Assertions.assertEquals(
      SchemaChange.NO_CHANGE,
      getSchemaChange(
        connectionReadNotBreaking,
        Optional.of(catalogId),
        Optional.of(ActorCatalogFetchEvent().withActorCatalogId(catalogId)),
      ),
    )
  }

  @Test
  fun testGetSchemaChangeBreaking() {
    val sourceId = UUID.randomUUID()
    val connectionReadWithSourceId = ConnectionRead().sourceCatalogId(UUID.randomUUID()).sourceId(sourceId).breakingChange(true)

    Assertions.assertEquals(
      SchemaChange.BREAKING,
      getSchemaChange(
        connectionReadWithSourceId,
        Optional.of(UUID.randomUUID()),
        Optional.empty<ActorCatalogFetchEvent>(),
      ),
    )
  }

  @Test
  fun testGetSchemaChangeNotBreaking() {
    val catalogId = UUID.randomUUID()
    val differentCatalogId = UUID.randomUUID()
    val connectionReadWithSourceId =
      ConnectionRead().breakingChange(false)

    Assertions.assertEquals(
      SchemaChange.NON_BREAKING,
      getSchemaChange(
        connectionReadWithSourceId,
        Optional.of(catalogId),
        Optional.of(ActorCatalogFetchEvent().withActorCatalogId(differentCatalogId)),
      ),
    )
  }

  @Test
  fun testParseSortKeyAllValues() {
    // Test null sort key defaults to CONNECTION_NAME ascending
    val result = parseSortKey(null)
    Assertions.assertEquals(SortKey.CONNECTION_NAME, result.sortKey)
    Assertions.assertTrue(result.ascending)

    // Test all sort key enum values
    Assertions.assertEquals(
      SortKeyInfo(SortKey.CONNECTION_NAME, true),
      parseSortKey(WebBackendConnectionListSortKey.CONNECTION_NAME_ASC),
    )
    Assertions.assertEquals(
      SortKeyInfo(SortKey.CONNECTION_NAME, false),
      parseSortKey(WebBackendConnectionListSortKey.CONNECTION_NAME_DESC),
    )
    Assertions.assertEquals(
      SortKeyInfo(SortKey.SOURCE_NAME, true),
      parseSortKey(WebBackendConnectionListSortKey.SOURCE_NAME_ASC),
    )
    Assertions.assertEquals(
      SortKeyInfo(SortKey.SOURCE_NAME, false),
      parseSortKey(WebBackendConnectionListSortKey.SOURCE_NAME_DESC),
    )
    Assertions.assertEquals(
      SortKeyInfo(SortKey.DESTINATION_NAME, true),
      parseSortKey(WebBackendConnectionListSortKey.DESTINATION_NAME_ASC),
    )
    Assertions.assertEquals(
      SortKeyInfo(SortKey.DESTINATION_NAME, false),
      parseSortKey(WebBackendConnectionListSortKey.DESTINATION_NAME_DESC),
    )
    Assertions.assertEquals(
      SortKeyInfo(SortKey.LAST_SYNC, true),
      parseSortKey(WebBackendConnectionListSortKey.LAST_SYNC_ASC),
    )
    Assertions.assertEquals(
      SortKeyInfo(SortKey.LAST_SYNC, false),
      parseSortKey(WebBackendConnectionListSortKey.LAST_SYNC_DESC),
    )
  }

  @Test
  fun testBuildConnectionFiltersEmpty() {
    val filters = WebBackendConnectionListFilters()
    val result = buildFilters(filters)

    Assertions.assertNotNull(result)
    Assertions.assertNull(result!!.searchTerm)

    // Empty lists are returned instead of null for collection fields
    Assertions.assertTrue(result.sourceDefinitionIds!!.isEmpty())
    Assertions.assertTrue(result.destinationDefinitionIds!!.isEmpty())
    Assertions.assertTrue(result.statuses!!.isEmpty())
    Assertions.assertTrue(result.states!!.isEmpty())
    Assertions.assertTrue(result.tagIds!!.isEmpty())
  }

  @ParameterizedTest
  @MethodSource("provideFilterTestCases")
  fun testBuildConnectionFiltersWithAllFields(
    testName: String?,
    filters: WebBackendConnectionListFilters?,
    expectedSearchTerm: String?,
    expectedSourceDefIds: Int,
    expectedDestDefIds: Int,
    expectedStatuses: Int,
    expectedStates: Int,
    expectedTagIds: Int,
  ) {
    val result = buildFilters(filters)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(expectedSearchTerm, result!!.searchTerm)
    Assertions.assertEquals(expectedSourceDefIds, result.sourceDefinitionIds!!.size)
    Assertions.assertEquals(expectedDestDefIds, result.destinationDefinitionIds!!.size)
    Assertions.assertEquals(expectedStatuses, result.statuses!!.size)
    Assertions.assertEquals(expectedStates, result.states!!.size)
    Assertions.assertEquals(expectedTagIds, result.tagIds!!.size)
  }

  companion object {
    private const val STREAM1 = "stream1"
    private const val STREAM2 = "stream2"
    private const val FIELD1 = "field1"
    private const val FIELD2 = "field2"
    private const val FIELD3 = "field3"
    private const val FIELD5 = "field5"

    private const val ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/destination-test/latest/icon.svg"

    @JvmStatic
    fun provideFilterTestCases(): Array<Array<Any?>?> {
      val sourceDefId1 = UUID.randomUUID()
      val sourceDefId2 = UUID.randomUUID()
      val destDefId = UUID.randomUUID()
      val tagId1 = UUID.randomUUID()
      val tagId2 = UUID.randomUUID()

      return arrayOf( // Search term only
        arrayOf(
          "Search term only",
          WebBackendConnectionListFilters().searchTerm("test search"),
          "test search",
          0,
          0,
          0,
          0,
          0,
        ), // Source definition IDs only
        arrayOf(
          "Source definition IDs only",
          WebBackendConnectionListFilters().sourceDefinitionIds(listOf<UUID?>(sourceDefId1, sourceDefId2)),
          null,
          2,
          0,
          0,
          0,
          0,
        ), // Destination definition IDs only
        arrayOf(
          "Destination definition IDs only",
          WebBackendConnectionListFilters().destinationDefinitionIds(listOf<UUID?>(destDefId)),
          null,
          0,
          1,
          0,
          0,
          0,
        ), // Tag IDs only
        arrayOf(
          "Tag IDs only",
          WebBackendConnectionListFilters().tagIds(listOf<UUID?>(tagId1, tagId2)),
          null,
          0,
          0,
          0,
          0,
          2,
        ), // Statuses only
        arrayOf(
          "Statuses only",
          WebBackendConnectionListFilters().statuses(
            listOf<StatusesEnum?>(
              StatusesEnum.HEALTHY,
              StatusesEnum.FAILED,
            ),
          ),
          null,
          0,
          0,
          2,
          0,
          0,
        ), // States only
        arrayOf(
          "States only",
          WebBackendConnectionListFilters().states(
            listOf<ActorStatus?>(
              ActorStatus.ACTIVE,
              ActorStatus.INACTIVE,
            ),
          ),
          null,
          0,
          0,
          0,
          2,
          0,
        ), // Multiple filters combined
        arrayOf(
          "Multiple filters combined",
          WebBackendConnectionListFilters()
            .searchTerm("production")
            .sourceDefinitionIds(listOf<UUID?>(sourceDefId1))
            .destinationDefinitionIds(listOf<UUID?>(destDefId))
            .statuses(listOf<StatusesEnum?>(StatusesEnum.HEALTHY))
            .states(listOf<ActorStatus?>(ActorStatus.ACTIVE))
            .tagIds(listOf<UUID?>(tagId1)),
          "production",
          1,
          1,
          1,
          1,
          1,
        ),
      )
    }
  }
}
