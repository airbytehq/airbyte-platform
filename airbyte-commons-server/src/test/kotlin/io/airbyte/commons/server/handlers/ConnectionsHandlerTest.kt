/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.analytics.TrackingClient
import io.airbyte.api.model.generated.ActorDefinitionRequestBody
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStream
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.ConfiguredStreamMapper
import io.airbyte.api.model.generated.ConnectionAutoPropagateResult
import io.airbyte.api.model.generated.ConnectionAutoPropagateSchemaChange
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionDataHistoryRequestBody
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamReadItem
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamRequestBody
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionSchedule
import io.airbyte.api.model.generated.ConnectionScheduleData
import io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule
import io.airbyte.api.model.generated.ConnectionScheduleDataCron
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionSearch
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.ConnectionStatusRead
import io.airbyte.api.model.generated.ConnectionStatusesRequestBody
import io.airbyte.api.model.generated.ConnectionStreamHistoryReadItem
import io.airbyte.api.model.generated.ConnectionStreamHistoryRequestBody
import io.airbyte.api.model.generated.ConnectionSyncStatus
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead
import io.airbyte.api.model.generated.DestinationSearch
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.FieldAdd
import io.airbyte.api.model.generated.FieldTransform
import io.airbyte.api.model.generated.JobAggregatedStats
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.JobSyncResultRead
import io.airbyte.api.model.generated.JobWithAttemptsRead
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.ResourceRequirements
import io.airbyte.api.model.generated.SchemaChangeBackfillPreference
import io.airbyte.api.model.generated.SelectedFieldInfo
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.model.generated.SourceSearch
import io.airbyte.api.model.generated.StreamMapperType
import io.airbyte.api.model.generated.StreamStats
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.api.model.generated.StreamTransformUpdateStream
import io.airbyte.api.model.generated.Tag
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.problems.model.generated.MapperValidationProblemResponse
import io.airbyte.api.problems.model.generated.ProblemMapperErrorData
import io.airbyte.api.problems.model.generated.ProblemMapperErrorDataMapper
import io.airbyte.api.problems.throwable.generated.ConnectionConflictingStreamProblem
import io.airbyte.api.problems.throwable.generated.ConnectionDoesNotSupportFileTransfersProblem
import io.airbyte.api.problems.throwable.generated.ConnectionLockedProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogInvalidAdditionalFieldProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogInvalidOperationProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogInvalidPrimaryKeyProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogMissingObjectNameProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogMissingPrimaryKeyProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogMissingRequiredFieldProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogRequiredProblem
import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.api.problems.throwable.generated.MapperValidationProblem
import io.airbyte.api.problems.throwable.generated.StreamDoesNotSupportFileTransfersProblem
import io.airbyte.commons.converters.toServerApi
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.FasterSyncFrequencyEntitlement
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.enums.isCompatible
import io.airbyte.commons.jackson.MoreMappers.initMapper
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.ConfigurationUpdate
import io.airbyte.commons.server.converters.ConnectionHelper
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.handlers.helpers.ConnectionScheduleHelper
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.handlers.helpers.ContextBuilder
import io.airbyte.commons.server.handlers.helpers.MapperSecretHelper
import io.airbyte.commons.server.handlers.helpers.NotificationHelper
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper.getJobIdToJobWithAttemptsReadMap
import io.airbyte.commons.server.helpers.ConnectionHelpers
import io.airbyte.commons.server.helpers.ConnectionHelpers.FIELD_NAME
import io.airbyte.commons.server.helpers.ConnectionHelpers.SECOND_FIELD_NAME
import io.airbyte.commons.server.helpers.ConnectionHelpers.connectionReadFromStandardSync
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateAirbyteCatalogWithTwoFields
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateApiCatalogWithTwoFields
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateBasicApiCatalog
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateBasicConfiguredAirbyteCatalog
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateBasicConnectionSchedule
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateBasicConnectionScheduleData
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateBasicJsonSchema
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateBasicSchedule
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateBasicScheduleData
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateExpectedConnectionRead
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateMultipleStreamsApiCatalog
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateMultipleStreamsConfiguredAirbyteCatalog
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.commons.server.services.CatalogDiffService
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.commons.server.validation.CatalogValidator
import io.airbyte.commons.server.validation.ValidationError
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorCatalogWithUpdatedAt
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.AttemptStatus
import io.airbyte.config.AttemptWithJobInfo.Companion.fromJob
import io.airbyte.config.BasicSchedule
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Configs
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConnectionContext
import io.airbyte.config.Cron
import io.airbyte.config.DestinationCatalog
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationOperation
import io.airbyte.config.FailureReason
import io.airbyte.config.FieldSelectionData
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobOutput
import io.airbyte.config.JobResetConnectionConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.MapperConfig
import io.airbyte.config.MapperOperationName
import io.airbyte.config.NotificationSettings
import io.airbyte.config.RefreshConfig
import io.airbyte.config.RefreshStream
import io.airbyte.config.ResetSourceConfiguration
import io.airbyte.config.Schedule
import io.airbyte.config.ScheduleData
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.StatusReason
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamDescriptorForDestination
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncStats
import io.airbyte.config.helpers.CatalogHelpers.Companion.createAirbyteStream
import io.airbyte.config.helpers.CronExpressionHelper
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.mapper.configs.HashingConfig
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.config.persistence.StatePersistence
import io.airbyte.config.persistence.StreamGenerationRepository
import io.airbyte.config.persistence.domain.Generation
import io.airbyte.config.persistence.helper.CatalogGenerationSetter
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.StreamStatusesService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.services.entitlements.ConnectorConfigEntitlementService
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.featureflag.EnableDestinationCatalogValidation
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.ResetStreamsStateWhenDisabled
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.ValidateConflictingDestinationStreams
import io.airbyte.featureflag.Workspace
import io.airbyte.mappers.helpers.createHashingMapper
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.mappers.transformations.DestinationCatalogGenerator.CatalogGenerationResult
import io.airbyte.mappers.transformations.DestinationCatalogGenerator.MapperError
import io.airbyte.mappers.transformations.DestinationCatalogGenerator.MapperErrorType
import io.airbyte.mappers.transformations.HashingMapper
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.Field
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import jakarta.validation.Valid
import org.junit.Assert
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.mock
import org.mockito.Mockito.mockStatic
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.Arrays
import java.util.Collections
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.function.Supplier
import java.util.stream.Collectors
import java.util.stream.Stream

internal class ConnectionsHandlerTest {
  private lateinit var jobPersistence: JobPersistence
  private lateinit var uuidGenerator: Supplier<UUID>
  private lateinit var connectionsHandler: ConnectionsHandler
  private lateinit var matchSearchHandler: MatchSearchHandler
  private lateinit var workspaceId: UUID
  private lateinit var organizationId: UUID
  private lateinit var sourceId: UUID
  private lateinit var destinationId: UUID
  private lateinit var sourceDefinitionId: UUID
  private lateinit var destinationDefinitionId: UUID
  private lateinit var source: SourceConnection
  private lateinit var destination: DestinationConnection
  private lateinit var standardSync: StandardSync
  private lateinit var standardSync2: StandardSync
  private lateinit var standardSyncDeleted: StandardSync
  private lateinit var standardSyncLocked: StandardSync
  private lateinit var connectionId: UUID
  private lateinit var connection2Id: UUID
  private lateinit var operationId: UUID
  private lateinit var otherOperationId: UUID
  private lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var trackingClient: TrackingClient
  private lateinit var eventRunner: EventRunner
  private lateinit var connectionHelper: ConnectionHelper
  private lateinit var featureFlagClient: TestClient
  private lateinit var entitlementService: EntitlementService
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater
  private lateinit var connectorDefinitionSpecificationHandler: ConnectorDefinitionSpecificationHandler

  private lateinit var jsonSchemaValidator: JsonSchemaValidator
  private lateinit var secretsProcessor: JsonSecretsProcessor
  private lateinit var configurationUpdate: ConfigurationUpdate
  private lateinit var oAuthConfigSupplier: OAuthConfigSupplier
  private lateinit var destinationService: DestinationService

  private lateinit var secretsRepositoryReader: SecretsRepositoryReader
  private lateinit var sourceService: SourceService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var secretPersistenceService: SecretPersistenceService
  private lateinit var actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper
  private lateinit var mapperSecretHelper: MapperSecretHelper

  private lateinit var destinationHandler: DestinationHandler
  private lateinit var sourceHandler: SourceHandler
  private lateinit var streamRefreshesHandler: StreamRefreshesHandler
  private lateinit var job: Job
  private lateinit var streamGenerationRepository: StreamGenerationRepository
  private lateinit var catalogGenerationSetter: CatalogGenerationSetter
  private lateinit var catalogValidator: CatalogValidator
  private lateinit var notificationHelper: NotificationHelper
  private lateinit var streamStatusesService: StreamStatusesService
  private lateinit var connectionTimelineEventService: ConnectionTimelineEventService
  private lateinit var connectionTimelineEventHelper: ConnectionTimelineEventHelper
  private lateinit var statePersistence: StatePersistence
  private lateinit var catalogService: CatalogService
  private lateinit var connectionService: ConnectionService
  private lateinit var destinationCatalogGenerator: DestinationCatalogGenerator
  private lateinit var catalogDiffService: CatalogDiffService
  private lateinit var connectionSchedulerHelper: ConnectionScheduleHelper
  private lateinit var licenseEntitlementChecker: LicenseEntitlementChecker
  private lateinit var connectorConfigEntitlementService: ConnectorConfigEntitlementService
  private lateinit var contextBuilder: ContextBuilder
  private val catalogConverter = CatalogConverter(FieldGenerator(), listOf(HashingMapper(initMapper())))
  private val applySchemaChangeHelper = ApplySchemaChangeHelper(catalogConverter)
  private val apiPojoConverters = ApiPojoConverters(catalogConverter)
  private val cronExpressionHelper = CronExpressionHelper()
  private lateinit var metricClient: MetricClient
  private lateinit var secretsRepositoryWriter: SecretsRepositoryWriter
  private lateinit var secretStorageService: SecretStorageService
  private lateinit var secretReferenceService: SecretReferenceService
  private lateinit var currentUserService: CurrentUserService
  private lateinit var fieldGenerator: FieldGenerator
  private lateinit var catalogMergeHelper: io.airbyte.commons.server.handlers.helpers.CatalogMergeHelper

  @Suppress("UNCHECKED_CAST")
  @BeforeEach
  fun setUp() {
    workspaceId = UUID.randomUUID()
    organizationId = UUID.randomUUID()
    sourceId = UUID.randomUUID()
    destinationId = UUID.randomUUID()
    sourceDefinitionId = UUID.randomUUID()
    destinationDefinitionId = UUID.randomUUID()
    connectionId = UUID.randomUUID()
    connection2Id = UUID.randomUUID()
    operationId = UUID.randomUUID()
    otherOperationId = UUID.randomUUID()

    source =
      SourceConnection()
        .withSourceId(sourceId)
        .withSourceDefinitionId(sourceDefinitionId)
        .withWorkspaceId(workspaceId)
        .withName("presto")

    destination =
      DestinationConnection()
        .withDestinationId(destinationId)
        .withDestinationDefinitionId(destinationDefinitionId)
        .withWorkspaceId(workspaceId)
        .withName("hudi")
        .withConfiguration(jsonNode(mapOf("apiKey" to "123-abc")))

    standardSync =
      StandardSync()
        .withConnectionId(connectionId)
        .withName(prestoToHudi)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix(prestoToHudiPrefix)
        .withStatus(StandardSync.Status.ACTIVE)
        .withCatalog(generateBasicConfiguredAirbyteCatalog())
        .withFieldSelectionData(FieldSelectionData().withAdditionalProperty(streamSelectionData, false))
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withOperationIds(listOf(operationId))
        .withManual(false)
        .withSchedule(generateBasicSchedule())
        .withScheduleType(StandardSync.ScheduleType.BASIC_SCHEDULE)
        .withScheduleData(generateBasicScheduleData())
        .withResourceRequirements(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS)
        .withSourceCatalogId(UUID.randomUUID())
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(true)
        .withBreakingChange(false)
        .withBackfillPreference(StandardSync.BackfillPreference.ENABLED)

    standardSync2 =
      StandardSync()
        .withConnectionId(connection2Id)
        .withName(prestoToHudi)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix(prestoToHudiPrefix)
        .withStatus(StandardSync.Status.ACTIVE)
        .withCatalog(generateBasicConfiguredAirbyteCatalog())
        .withFieldSelectionData(FieldSelectionData().withAdditionalProperty(streamSelectionData, false))
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withOperationIds(listOf(operationId))
        .withManual(false)
        .withSchedule(generateBasicSchedule())
        .withScheduleType(StandardSync.ScheduleType.BASIC_SCHEDULE)
        .withScheduleData(generateBasicScheduleData())
        .withResourceRequirements(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS)
        .withSourceCatalogId(UUID.randomUUID())
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(true)
        .withBreakingChange(false)

    standardSyncDeleted =
      StandardSync()
        .withConnectionId(connectionId)
        .withName("presto to hudi2")
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix("presto_to_hudi2")
        .withStatus(StandardSync.Status.DEPRECATED)
        .withCatalog(generateBasicConfiguredAirbyteCatalog())
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withOperationIds(listOf(operationId))
        .withManual(false)
        .withSchedule(generateBasicSchedule())
        .withResourceRequirements(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS)

    standardSyncLocked =
      StandardSync()
        .withConnectionId(connectionId)
        .withName("presto to hudi2")
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix("presto_to_hudi2")
        .withStatus(StandardSync.Status.LOCKED)
        .withCatalog(generateBasicConfiguredAirbyteCatalog())
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withOperationIds(listOf(operationId))
        .withManual(false)
        .withSchedule(generateBasicSchedule())
        .withResourceRequirements(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS)

    jobPersistence = mock()
    streamRefreshesHandler = mock()
    catalogService = mock()
    uuidGenerator = mock()
    workspaceHelper = mock()
    trackingClient = mock()
    eventRunner = mock()
    connectionHelper = mock()
    actorDefinitionVersionHelper = mock()
    actorDefinitionVersionUpdater = mock()
    connectorDefinitionSpecificationHandler = mock()
    jsonSchemaValidator = mock()
    secretsProcessor = mock()
    configurationUpdate = mock()
    oAuthConfigSupplier = mock()
    destinationService = mock()
    connectionService = mock()
    secretsRepositoryReader = mock()
    sourceService = mock()
    workspaceService = mock()
    secretPersistenceService = mock()
    actorDefinitionHandlerHelper = mock()
    streamStatusesService = mock()
    connectionTimelineEventService = mock()
    connectionTimelineEventHelper = mock()
    statePersistence = mock()
    mapperSecretHelper = mock()
    licenseEntitlementChecker = mock()
    connectorConfigEntitlementService = mock()
    contextBuilder = mock()
    featureFlagClient = mock()
    entitlementService = mock()
    metricClient = mock()
    secretsRepositoryWriter = mock()
    secretStorageService = mock()
    secretReferenceService = mock()
    currentUserService = mock()
    job = mock()
    streamGenerationRepository = mock()
    catalogGenerationSetter = mock()
    catalogValidator = mock()
    notificationHelper = mock()
    destinationCatalogGenerator = mock()
    fieldGenerator = mock()
    catalogMergeHelper =
      io.airbyte.commons.server.handlers.helpers
        .CatalogMergeHelper(fieldGenerator)
    catalogDiffService =
      CatalogDiffService(
        catalogService,
        connectionService,
        applySchemaChangeHelper,
        catalogConverter,
        catalogMergeHelper,
      )

    connectionSchedulerHelper =
      ConnectionScheduleHelper(
        apiPojoConverters,
        cronExpressionHelper,
        featureFlagClient,
        entitlementService,
        workspaceHelper,
      )

    connectionsHandler =
      ConnectionsHandler(
        streamRefreshesHandler,
        jobPersistence,
        catalogService,
        uuidGenerator,
        workspaceHelper,
        trackingClient,
        eventRunner,
        connectionHelper,
        featureFlagClient,
        actorDefinitionVersionHelper,
        connectorDefinitionSpecificationHandler,
        streamGenerationRepository,
        catalogGenerationSetter,
        catalogValidator,
        notificationHelper,
        streamStatusesService,
        connectionTimelineEventService,
        connectionTimelineEventHelper,
        statePersistence,
        sourceService,
        destinationService,
        connectionService,
        workspaceService,
        destinationCatalogGenerator,
        catalogConverter,
        applySchemaChangeHelper,
        apiPojoConverters,
        connectionSchedulerHelper,
        mapperSecretHelper,
        metricClient,
        licenseEntitlementChecker,
        contextBuilder,
        catalogDiffService,
      )

    destinationHandler =
      DestinationHandler(
        jsonSchemaValidator,
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
        jsonSchemaValidator,
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
        mock(),
      )

    matchSearchHandler =
      MatchSearchHandler(
        destinationHandler,
        sourceHandler,
        sourceService,
        destinationService,
        connectionService,
        apiPojoConverters,
      )

    whenever(workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(sourceId)).thenReturn(workspaceId)
    whenever(workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(destinationId)).thenReturn(workspaceId)
    whenever(workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(operationId)).thenReturn(workspaceId)
    whenever(workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(otherOperationId)).thenReturn(workspaceId)
    whenever(workspaceHelper.getOrganizationForWorkspace(workspaceId)).thenReturn(organizationId)

    whenever(destinationCatalogGenerator.generateDestinationCatalog(anyOrNull()))
      .thenReturn(CatalogGenerationResult(ConfiguredAirbyteCatalog(), emptyMap()))

    whenever(mapperSecretHelper.maskMapperSecrets(anyOrNull())).thenAnswer { it.getArgument(0) }
    whenever(mapperSecretHelper.createAndReplaceMapperSecrets(anyOrNull(), anyOrNull())).thenAnswer { it.getArgument(1) }
    whenever(mapperSecretHelper.updateAndReplaceMapperSecrets(anyOrNull(), anyOrNull(), anyOrNull())).thenAnswer { it.getArgument(2) }
  }

  @Test
  fun testListConnectionEventsForJob() {
    val jobId = 123L
    val mockEvents =
      listOf(
        ConnectionTimelineEvent(
          id = UUID.randomUUID(),
          connectionId = connectionId,
          eventType = "SYNC_STARTED",
          summary = "{}",
          userId = null,
          createdAt = java.time.OffsetDateTime.now(),
        ),
      )

    whenever(connectionTimelineEventService.listEventsForJob(jobId)).thenReturn(mockEvents)

    val result = connectionsHandler.listConnectionEventsForJob(jobId)

    assertNotNull(result)
    assertEquals(1, result.events.size)
    assertEquals(connectionId, result.events[0].connectionId)
    verify(connectionTimelineEventService).listEventsForJob(jobId)
  }

  @Nested
  internal inner class UnMockedConnectionHelper {
    @BeforeEach
    fun setUp() {
      whenever(uuidGenerator.get()).thenReturn(standardSync.connectionId)
      val sourceDefinition =
        StandardSourceDefinition()
          .withName(sourceTest)
          .withSourceDefinitionId(UUID.randomUUID())
      val destinationDefinition =
        StandardDestinationDefinition()
          .withName(destinationTest)
          .withDestinationDefinitionId(UUID.randomUUID())
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)
      whenever(sourceService.getSourceDefinitionFromConnection(standardSync.connectionId)).thenReturn(
        sourceDefinition,
      )
      whenever(destinationService.getDestinationDefinitionFromConnection(standardSync.connectionId))
        .thenReturn(
          destinationDefinition,
        )
      whenever(sourceService.getSourceConnection(source.sourceId))
        .thenReturn(source)
      whenever(destinationService.getDestinationConnection(destination.destinationId))
        .thenReturn(destination)
      whenever(connectionService.getStandardSync(connectionId)).thenReturn(standardSync)
      whenever(jobPersistence.getLastReplicationJob(connectionId)).thenReturn(Optional.of<Job>(job))
      whenever(jobPersistence.getFirstReplicationJob(connectionId)).thenReturn(Optional.of<Job>(job))
    }

    @Test
    fun testGetConnection() {
      whenever(connectionService.getStandardSync(standardSync.connectionId))
        .thenReturn(standardSync)

      val actualConnectionRead = connectionsHandler.getConnection(standardSync.connectionId)

      assertEquals(generateExpectedConnectionRead(standardSync), actualConnectionRead)
    }

    @Test
    fun testGetConnectionForJob() {
      val jobId = 456L

      whenever(connectionService.getStandardSync(standardSync.connectionId))
        .thenReturn(standardSync)
      whenever(jobPersistence.getJob(jobId)).thenReturn(
        Job(
          jobId,
          ConfigType.SYNC,
          scope,
          JobConfig(),
          mutableListOf(),
          JobStatus.PENDING,
          null,
          0,
          0,
          true,
        ),
      )
      val generations = listOf(Generation("name", null, 1))
      whenever(streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(standardSync.connectionId))
        .thenReturn(generations)
      whenever(
        catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
          standardSync.catalog,
          jobId,
          mutableListOf(),
          generations,
        ),
      ).thenReturn(standardSync.catalog)

      val actualConnectionRead = connectionsHandler.getConnectionForJob(standardSync.connectionId, jobId)

      assertEquals(generateExpectedConnectionRead(standardSync), actualConnectionRead)
    }

    @Test
    fun testGetConnectionForJobWithRefresh() {
      val jobId = 456L

      val refreshStreamDescriptors =
        listOf(
          RefreshStream()
            .withRefreshType(RefreshStream.RefreshType.TRUNCATE)
            .withStreamDescriptor(StreamDescriptor().withName("name")),
        )

      val config =
        JobConfig()
          .withRefresh(RefreshConfig().withStreamsToRefresh(refreshStreamDescriptors))

      whenever(connectionService.getStandardSync(standardSync.connectionId))
        .thenReturn(standardSync)
      whenever(jobPersistence.getJob(jobId)).thenReturn(
        Job(
          jobId,
          ConfigType.REFRESH,
          scope,
          config,
          mutableListOf(),
          JobStatus.PENDING,
          null,
          0,
          0,
          true,
        ),
      )
      val generations = listOf(Generation("name", null, 1))
      whenever(streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(standardSync.connectionId))
        .thenReturn(generations)
      whenever(
        catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
          standardSync.catalog,
          jobId,
          refreshStreamDescriptors,
          generations,
        ),
      ).thenReturn(standardSync.catalog)

      val actualConnectionRead = connectionsHandler.getConnectionForJob(standardSync.connectionId, jobId)

      assertEquals(generateExpectedConnectionRead(standardSync), actualConnectionRead)
    }

    @Test
    fun testGetConnectionForClearJob() {
      val jobId = 456L

      val clearedStreamDescriptors =
        listOf(StreamDescriptor().withName("name"))

      val config =
        JobConfig()
          .withResetConnection(
            JobResetConnectionConfig().withResetSourceConfiguration(
              ResetSourceConfiguration().withStreamsToReset(clearedStreamDescriptors),
            ),
          )

      whenever(connectionService.getStandardSync(standardSync.connectionId))
        .thenReturn(standardSync)
      whenever(jobPersistence.getJob(jobId)).thenReturn(
        Job(
          jobId,
          ConfigType.RESET_CONNECTION,
          scope,
          config,
          mutableListOf(),
          JobStatus.PENDING,
          null,
          0,
          0,
          true,
        ),
      )
      val generations = listOf(Generation("name", null, 1))
      whenever(streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(standardSync.connectionId))
        .thenReturn(generations)
      whenever(
        catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformationForClear(
          standardSync.catalog,
          jobId,
          clearedStreamDescriptors.toSet(),
          generations,
        ),
      ).thenReturn(standardSync.catalog)

      val actualConnectionRead = connectionsHandler.getConnectionForJob(standardSync.connectionId, jobId)

      assertEquals(generateExpectedConnectionRead(standardSync), actualConnectionRead)
    }

    @Test
    fun testListConnectionsForWorkspace() {
      whenever(connectionService.listWorkspaceStandardSyncs(source.workspaceId, false))
        .thenReturn(listOf(standardSync, standardSyncLocked))
      whenever(connectionService.listWorkspaceStandardSyncs(source.workspaceId, true))
        .thenReturn(listOf(standardSync, standardSyncDeleted, standardSyncLocked))
      whenever(connectionService.getStandardSync(standardSync.connectionId))
        .thenReturn(standardSync)

      val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(source.workspaceId)
      val actualConnectionReadList = connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody)
      assertEquals(2, actualConnectionReadList.connections.size)
      assertEquals(
        generateExpectedConnectionRead(standardSync),
        actualConnectionReadList.connections[0],
      )

      val actualConnectionReadListWithDeleted = connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody, true)
      val connections = actualConnectionReadListWithDeleted.connections
      assertEquals(3, connections.size)
      assertEquals(apiPojoConverters.internalToConnectionRead(standardSync), connections[0])
      assertEquals(apiPojoConverters.internalToConnectionRead(standardSyncDeleted), connections[1])
      assertEquals(apiPojoConverters.internalToConnectionRead(standardSyncLocked), connections[2])
    }

    @Test
    fun testListConnections() {
      whenever(connectionService.listStandardSyncs())
        .thenReturn(listOf(standardSync))
      whenever(sourceService.getSourceConnection(source.sourceId))
        .thenReturn(source)
      whenever(connectionService.getStandardSync(standardSync.connectionId))
        .thenReturn(standardSync)

      val actualConnectionReadList = connectionsHandler.listConnections()

      assertEquals(
        generateExpectedConnectionRead(standardSync),
        actualConnectionReadList.connections[0],
      )
    }

    @Test
    fun testListConnectionsByActorDefinition() {
      whenever(
        connectionService.listConnectionsByActorDefinitionIdAndType(
          sourceDefinitionId,
          ActorType.SOURCE.value(),
          false,
          true,
        ),
      ).thenReturn(listOf(standardSync))
      whenever(
        connectionService.listConnectionsByActorDefinitionIdAndType(
          destinationDefinitionId,
          ActorType.DESTINATION.value(),
          false,
          true,
        ),
      ).thenReturn(listOf(standardSync2))

      val connectionReadListForSourceDefinitionId =
        connectionsHandler.listConnectionsForActorDefinition(
          ActorDefinitionRequestBody()
            .actorDefinitionId(sourceDefinitionId)
            .actorType(io.airbyte.api.model.generated.ActorType.SOURCE),
        )

      val connectionReadListForDestinationDefinitionId =
        connectionsHandler.listConnectionsForActorDefinition(
          ActorDefinitionRequestBody()
            .actorDefinitionId(destinationDefinitionId)
            .actorType(io.airbyte.api.model.generated.ActorType.DESTINATION),
        )

      assertEquals(
        listOf(generateExpectedConnectionRead(standardSync)),
        connectionReadListForSourceDefinitionId.connections,
      )
      assertEquals(
        listOf(generateExpectedConnectionRead(standardSync2)),
        connectionReadListForDestinationDefinitionId.connections,
      )
    }

    @Test
    fun testSearchConnections() {
      val connectionRead1 = generateExpectedConnectionRead(standardSync)
      val standardSync2 =
        StandardSync()
          .withConnectionId(UUID.randomUUID())
          .withName("test connection")
          .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
          .withNamespaceFormat("ns_format")
          .withPrefix("test_prefix")
          .withStatus(StandardSync.Status.ACTIVE)
          .withCatalog(generateBasicConfiguredAirbyteCatalog())
          .withSourceId(sourceId)
          .withDestinationId(destinationId)
          .withOperationIds(listOf<UUID?>(operationId))
          .withManual(true)
          .withResourceRequirements(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS)
          .withBreakingChange(false)
          .withNotifySchemaChanges(false)
          .withNotifySchemaChangesByEmail(true)
      val connectionRead2 = connectionReadFromStandardSync(standardSync2)
      val sourceDefinition =
        StandardSourceDefinition()
          .withName(sourceTest)
          .withSourceDefinitionId(UUID.randomUUID())
      val destinationDefinition =
        StandardDestinationDefinition()
          .withName(destinationTest)
          .withDestinationDefinitionId(UUID.randomUUID())
      val sourceVersion: ActorDefinitionVersion = mock(ActorDefinitionVersion::class.java)
      val destinationVersion: ActorDefinitionVersion = mock(ActorDefinitionVersion::class.java)

      whenever(connectionService.listStandardSyncs())
        .thenReturn(listOf(standardSync, standardSync2))
      whenever(sourceService.getSourceConnection(source.sourceId))
        .thenReturn(source)
      whenever(destinationService.getDestinationConnection(destination.destinationId))
        .thenReturn(destination)
      whenever(connectionService.getStandardSync(standardSync.connectionId))
        .thenReturn(standardSync)
      whenever(connectionService.getStandardSync(standardSync2.connectionId))
        .thenReturn(standardSync2)
      whenever(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
        .thenReturn(sourceDefinition)
      whenever(
        destinationService.getStandardDestinationDefinition(
          destination.destinationDefinitionId,
        ),
      ).thenReturn(destinationDefinition)
      whenever(
        actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
          sourceDefinition,
          source.workspaceId,
          source.sourceId,
        ),
      ).thenReturn(ActorDefinitionVersionWithOverrideStatus(sourceVersion, false))
      whenever(
        actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
          destinationDefinition,
          destination.workspaceId,
          destination.destinationId,
        ),
      ).thenReturn(ActorDefinitionVersionWithOverrideStatus(destinationVersion, false))

      val connectionSearch = ConnectionSearch()
      connectionSearch.namespaceDefinition(NamespaceDefinitionType.SOURCE)
      var actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(1, actualConnectionReadList.connections.size)
      assertEquals(connectionRead1, actualConnectionReadList.connections[0])

      connectionSearch.namespaceDefinition(null)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(2, actualConnectionReadList.connections.size)
      assertEquals(connectionRead1, actualConnectionReadList.connections[0])
      assertEquals(connectionRead2, actualConnectionReadList.connections[1])

      val sourceSearch = SourceSearch().sourceId(UUID.randomUUID())
      connectionSearch.setSource(sourceSearch)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(0, actualConnectionReadList.connections.size)

      sourceSearch.sourceId(connectionRead1.sourceId)
      connectionSearch.setSource(sourceSearch)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(2, actualConnectionReadList.connections.size)
      assertEquals(connectionRead1, actualConnectionReadList.connections[0])
      assertEquals(connectionRead2, actualConnectionReadList.connections[1])

      val destinationSearch = DestinationSearch()
      connectionSearch.setDestination(destinationSearch)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(2, actualConnectionReadList.connections.size)
      assertEquals(connectionRead1, actualConnectionReadList.connections[0])
      assertEquals(connectionRead2, actualConnectionReadList.connections[1])

      destinationSearch.connectionConfiguration(
        jsonNode(
          mapOf(
            "apiKey" to "not-found",
          ),
        ),
      )
      connectionSearch.setDestination(destinationSearch)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(0, actualConnectionReadList.connections.size)

      destinationSearch.connectionConfiguration(
        jsonNode(
          mapOf(
            "apiKey" to "123-abc",
          ),
        ),
      )
      connectionSearch.setDestination(destinationSearch)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(2, actualConnectionReadList.connections.size)
      assertEquals(connectionRead1, actualConnectionReadList.connections[0])
      assertEquals(connectionRead2, actualConnectionReadList.connections[1])

      connectionSearch.name("non-existent")
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(0, actualConnectionReadList.connections.size)

      connectionSearch.name(connectionRead1.name)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(1, actualConnectionReadList.connections.size)
      assertEquals(connectionRead1, actualConnectionReadList.connections[0])

      connectionSearch.name(connectionRead2.name)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(1, actualConnectionReadList.connections.size)
      assertEquals(connectionRead2, actualConnectionReadList.connections[0])

      connectionSearch.namespaceDefinition(connectionRead1.namespaceDefinition)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(0, actualConnectionReadList.connections.size)

      connectionSearch.name(null)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(1, actualConnectionReadList.connections.size)
      assertEquals(connectionRead1, actualConnectionReadList.connections[0])

      connectionSearch.namespaceDefinition(connectionRead2.namespaceDefinition)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(1, actualConnectionReadList.connections.size)
      assertEquals(connectionRead2, actualConnectionReadList.connections[0])

      connectionSearch.namespaceDefinition(null)
      connectionSearch.status(ConnectionStatus.INACTIVE)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(0, actualConnectionReadList.connections.size)

      connectionSearch.status(ConnectionStatus.ACTIVE)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(2, actualConnectionReadList.connections.size)
      assertEquals(connectionRead1, actualConnectionReadList.connections[0])
      assertEquals(connectionRead2, actualConnectionReadList.connections[1])

      connectionSearch.prefix(connectionRead1.prefix)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(1, actualConnectionReadList.connections.size)
      assertEquals(connectionRead1, actualConnectionReadList.connections[0])

      connectionSearch.prefix(connectionRead2.prefix)
      actualConnectionReadList = matchSearchHandler.searchConnections(connectionSearch)
      assertEquals(1, actualConnectionReadList.connections.size)
      assertEquals(connectionRead2, actualConnectionReadList.connections[0])
    }

    @Test
    fun testDeleteConnection() {
      connectionsHandler.deleteConnection(connectionId)

      verify(connectionHelper).deleteConnection(connectionId)
      verify(streamRefreshesHandler).deleteRefreshesForConnection(connectionId)
    }

    @Test
    fun failOnUnmatchedWorkspacesInCreate() {
      whenever(workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(standardSync.sourceId))
        .thenReturn(UUID.randomUUID())
      whenever(workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(standardSync.destinationId))
        .thenReturn(UUID.randomUUID())
      whenever(sourceService.getSourceConnection(source.sourceId))
        .thenReturn(source)
      whenever(destinationService.getDestinationConnection(destination.destinationId))
        .thenReturn(destination)

      whenever(uuidGenerator.get()).thenReturn(standardSync.connectionId)
      val sourceDefinition =
        StandardSourceDefinition()
          .withName(sourceTest)
          .withSourceDefinitionId(UUID.randomUUID())
      val destinationDefinition =
        StandardDestinationDefinition()
          .withName(destinationTest)
          .withDestinationDefinitionId(UUID.randomUUID())
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)
      whenever(sourceService.getSourceDefinitionFromConnection(standardSync.connectionId))
        .thenReturn(sourceDefinition)
      whenever(destinationService.getDestinationDefinitionFromConnection(standardSync.connectionId))
        .thenReturn(destinationDefinition)

      val catalog = generateBasicApiCatalog()

      val connectionCreate =
        ConnectionCreate()
          .sourceId(standardSync.sourceId)
          .destinationId(standardSync.destinationId)
          .operationIds(standardSync.operationIds)
          .name(prestoToHudi)
          .namespaceDefinition(NamespaceDefinitionType.SOURCE)
          .namespaceFormat(null)
          .prefix(prestoToHudiPrefix)
          .status(ConnectionStatus.ACTIVE)
          .schedule(generateBasicConnectionSchedule())
          .syncCatalog(catalog)
          .resourceRequirements(
            ResourceRequirements()
              .cpuRequest(standardSync.resourceRequirements.cpuRequest)
              .cpuLimit(standardSync.resourceRequirements.cpuLimit)
              .memoryRequest(standardSync.resourceRequirements.memoryRequest)
              .memoryLimit(standardSync.resourceRequirements.memoryLimit),
          )

      Assert.assertThrows(IllegalArgumentException::class.java) {
        connectionsHandler.createConnection(connectionCreate)
      }
    }

    @Test
    fun testEnumCompatibility() {
      assertTrue(isCompatible<StandardSync.Status, ConnectionStatus>())
      assertTrue(isCompatible<ConnectionSchedule.TimeUnitEnum, Schedule.TimeUnit>())
      assertTrue(isCompatible<io.airbyte.config.DestinationSyncMode, DestinationSyncMode>())
      assertTrue(isCompatible<io.airbyte.config.DataType, io.airbyte.api.model.generated.DataType>())
      assertTrue(isCompatible<StandardSync.NonBreakingChangesPreference, io.airbyte.api.model.generated.NonBreakingChangesPreference>())
      assertTrue(isCompatible<StandardSync.BackfillPreference, SchemaChangeBackfillPreference>())
      assertTrue(isCompatible<JobSyncConfig.NamespaceDefinitionType, NamespaceDefinitionType>())
    }

    @ParameterizedTest
    @CsvSource("true,false", "true,true", "false,true", "false,false")
    fun defaultsForIncludeFiles(
      sourceSupportsFiles: Boolean,
      destSupportsFiles: Boolean,
    ) {
      val sourceDefinitionVersion = ActorDefinitionVersion()
      sourceDefinitionVersion.setSupportsFileTransfer(sourceSupportsFiles)

      val destDefinitionVersion = ActorDefinitionVersion()
      destDefinitionVersion.setSupportsFileTransfer(destSupportsFiles)

      val streams: MutableList<AirbyteStreamAndConfiguration?> = mutableListOf()

      // file based and no include files value set
      val stream1 =
        AirbyteStreamAndConfiguration()
          .stream(AirbyteStream().name("stream1").isFileBased(true))
          .config(AirbyteStreamConfiguration())
      // not file based
      val stream2 =
        AirbyteStreamAndConfiguration()
          .stream(AirbyteStream().name("stream2").isFileBased(false))
          .config(AirbyteStreamConfiguration())
      // file based and include files set to false
      val stream3 =
        AirbyteStreamAndConfiguration()
          .stream(AirbyteStream().name("stream3").isFileBased(true))
          .config(AirbyteStreamConfiguration().includeFiles(false))
      // file based and include files set to true
      val stream4 =
        AirbyteStreamAndConfiguration()
          .stream(AirbyteStream().name("stream4").isFileBased(true))
          .config(AirbyteStreamConfiguration().includeFiles(true))
      // file based and no include files value set (same as stream1)
      val stream5 =
        AirbyteStreamAndConfiguration()
          .stream(AirbyteStream().name("stream5").isFileBased(true))
          .config(AirbyteStreamConfiguration())

      streams.add(stream1)
      streams.add(stream2)
      streams.add(stream3)
      streams.add(stream4)
      streams.add(stream5)
      val catalog = AirbyteCatalog().streams(streams)

      val actual = connectionsHandler.applyDefaultIncludeFiles(catalog, sourceDefinitionVersion, destDefinitionVersion)

      if (!sourceSupportsFiles) {
        // existing values are respected; others remain unset
        assertEquals(
          null,
          actual
            .streams[0]
            .config
            .includeFiles,
        )
        assertEquals(
          null,
          actual
            .streams[1]
            .config
            .includeFiles,
        )
        assertEquals(
          false,
          actual
            .streams[2]
            .config
            .includeFiles,
        )
        assertEquals(
          true,
          actual
            .streams[3]
            .config
            .includeFiles,
        )
        assertEquals(
          null,
          actual
            .streams[4]
            .config
            .includeFiles,
        )
      } else {
        // default to whether the destination supports; non-file based streams remain unset
        assertEquals(
          destSupportsFiles,
          actual
            .streams[0]
            .config
            .includeFiles,
        )
        assertEquals(
          null,
          actual
            .streams[1]
            .config
            .includeFiles,
        )
        assertEquals(
          false,
          actual
            .streams[2]
            .config
            .includeFiles,
        )
        assertEquals(
          true,
          actual
            .streams[3]
            .config
            .includeFiles,
        )
        assertEquals(
          destSupportsFiles,
          actual
            .streams[4]
            .config
            .includeFiles,
        )
      }
    }

    @Nested
    internal inner class CreateConnection {
      @BeforeEach
      fun setup() {
        // for create calls
        whenever(workspaceHelper.getWorkspaceForDestinationId(standardSync.destinationId)).thenReturn(workspaceId)
        // for update calls
        whenever(workspaceHelper.getWorkspaceForConnectionId(standardSync.connectionId)).thenReturn(workspaceId)
        whenever(workspaceHelper.getOrganizationForWorkspace(anyOrNull())).thenReturn(organizationId)
        val sourceVersion = mock(ActorDefinitionVersion::class.java)
        val destinationVersion = mock(ActorDefinitionVersion::class.java)
        whenever(sourceVersion.supportsFileTransfer).thenReturn(false)
        whenever(destinationVersion.supportsFileTransfer).thenReturn(false)
        whenever(
          actorDefinitionVersionHelper.getSourceVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(sourceVersion)
        whenever(
          actorDefinitionVersionHelper.getDestinationVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(destinationVersion)
      }

      private fun buildConnectionCreateRequest(
        standardSync: StandardSync,
        catalog: AirbyteCatalog?,
      ): ConnectionCreate =
        ConnectionCreate()
          .sourceId(standardSync.sourceId)
          .destinationId(standardSync.destinationId)
          .operationIds(standardSync.operationIds)
          .name(prestoToHudi)
          .namespaceDefinition(NamespaceDefinitionType.SOURCE)
          .namespaceFormat(null)
          .prefix(prestoToHudiPrefix)
          .status(ConnectionStatus.ACTIVE)
          .schedule(generateBasicConnectionSchedule())
          .syncCatalog(catalog)
          .resourceRequirements(
            ResourceRequirements()
              .cpuRequest(standardSync.resourceRequirements.cpuRequest)
              .cpuLimit(standardSync.resourceRequirements.cpuLimit)
              .memoryRequest(standardSync.resourceRequirements.memoryRequest)
              .memoryLimit(standardSync.resourceRequirements.memoryLimit),
          ).sourceCatalogId(standardSync.sourceCatalogId)
          .destinationCatalogId(standardSync.destinationCatalogId)
          .notifySchemaChanges(standardSync.notifySchemaChanges)
          .notifySchemaChangesByEmail(standardSync.notifySchemaChangesByEmail)
          .backfillPreference(
            standardSync.backfillPreference.convertTo<SchemaChangeBackfillPreference>(),
          )

      @Test
      fun testCreateConnection() {
        val catalog = generateBasicApiCatalog()

        val workspace =
          StandardWorkspace()
            .withWorkspaceId(workspaceId)
        whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace)

        val connectionCreate = buildConnectionCreateRequest(standardSync, catalog)

        val actualConnectionRead = connectionsHandler.createConnection(connectionCreate)

        val expectedConnectionRead = generateExpectedConnectionRead(standardSync)

        assertEquals(expectedConnectionRead, actualConnectionRead)

        verify(connectionService).writeStandardSync(
          standardSync
            .withNotifySchemaChangesByEmail(null),
        )

        // Use new schedule schema, verify that we get the same results.
        connectionCreate
          .schedule(null)
          .scheduleType(ConnectionScheduleType.BASIC)
          .scheduleData(generateBasicConnectionScheduleData())
        assertEquals(
          expectedConnectionRead
            .notifySchemaChangesByEmail(null),
          connectionsHandler.createConnection(connectionCreate),
        )
      }

      @ParameterizedTest
      @ValueSource(strings = ["SOURCE_CONNECTOR", "DESTINATION_CONNECTOR"])
      fun testCreateConnectionWithUnentitledSourceShouldThrow(entitlement: Entitlement) {
        val catalog = generateBasicApiCatalog()

        val workspace =
          StandardWorkspace()
            .withWorkspaceId(workspaceId)
        whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace)

        val connectionCreate = buildConnectionCreateRequest(standardSync, catalog)

        doThrow(LicenseEntitlementProblem())
          .`when`(licenseEntitlementChecker)
          .ensureEntitled(anyOrNull(), eq(entitlement), anyOrNull())

        assertThrows(
          LicenseEntitlementProblem::class.java,
        ) { connectionsHandler.createConnection(connectionCreate) }

        verifyNoInteractions(connectionService)
      }

      @Test
      fun testCreateConnectionWithDuplicateStreamsShouldThrowException() {
        val catalog = generateMultipleStreamsApiCatalog(false, false, 2)

        val connectionCreate = buildConnectionCreateRequest(standardSync, catalog)

        assertThrows(
          IllegalArgumentException::class.java,
        ) { connectionsHandler.createConnection(connectionCreate) }
      }

      @Test
      fun testCreateConnectionWithSelectedFields() {
        val workspace =
          StandardWorkspace()
            .withWorkspaceId(workspaceId)
        whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace)

        val catalogWithSelectedFields = generateApiCatalogWithTwoFields()
        // Only select one of the two fields.
        catalogWithSelectedFields
          .streams
          .get(0)
          .config
          .fieldSelectionEnabled(true)
          .selectedFields(listOf<@Valid SelectedFieldInfo?>(SelectedFieldInfo().addFieldPathItem(FIELD_NAME)))

        val connectionCreate = buildConnectionCreateRequest(standardSync, catalogWithSelectedFields)

        val actualConnectionRead = connectionsHandler.createConnection(connectionCreate)

        val expectedConnectionRead = generateExpectedConnectionRead(standardSync)

        assertEquals(expectedConnectionRead, actualConnectionRead)

        standardSync.withFieldSelectionData(FieldSelectionData().withAdditionalProperty(streamSelectionData, true))

        verify(connectionService).writeStandardSync(standardSync.withNotifySchemaChangesByEmail(null))
      }

      @Test
      fun testCreateConnectionWithHashedFields() {
        val workspace =
          StandardWorkspace()
            .withWorkspaceId(workspaceId)
        whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace)

        val catalog = generateBasicApiCatalog()
        catalog
          .streams
          .first()
          .config
          .hashedFields(listOf<@Valid SelectedFieldInfo?>(SelectedFieldInfo().addFieldPathItem(FIELD_NAME)))

        val connectionCreate = buildConnectionCreateRequest(standardSync, catalog)

        val actualConnectionRead = connectionsHandler.createConnection(connectionCreate)

        val expectedConnectionRead = generateExpectedConnectionRead(standardSync)
        assertEquals(expectedConnectionRead, actualConnectionRead)

        standardSync
          .catalog
          .streams
          .first()
          .mappers = listOf(createHashingMapper(FIELD_NAME))
        verify(connectionService).writeStandardSync(standardSync.withNotifySchemaChangesByEmail(null))
      }

      @Test
      fun testCreateConnectionWithMappers() {
        val workspace =
          StandardWorkspace()
            .withWorkspaceId(workspaceId)
        whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace)

        val newMapperId = UUID.randomUUID()
        whenever(uuidGenerator.get()).thenReturn(connectionId, newMapperId)
        val hashingMapper: MapperConfig = createHashingMapper(FIELD_NAME, newMapperId)

        val catalog = generateBasicApiCatalog()
        catalog.streams.first().config.mappers(
          listOf<@Valid ConfiguredStreamMapper?>(
            ConfiguredStreamMapper()
              .type(StreamMapperType.HASHING)
              .mapperConfiguration(jsonNode<Any?>(hashingMapper.config())),
          ),
        )

        val connectionCreate = buildConnectionCreateRequest(standardSync, catalog)

        val actualConnectionRead = connectionsHandler.createConnection(connectionCreate)

        val expectedConnectionRead = generateExpectedConnectionRead(standardSync)
        assertEquals(expectedConnectionRead, actualConnectionRead)

        standardSync
          .catalog
          .streams
          .first()
          .mappers = listOf(hashingMapper)
        verify(connectionService).writeStandardSync(standardSync.withNotifySchemaChangesByEmail(null))
      }

      @Test
      fun testCreateConnectionWithDestinationCatalog() {
        val workspace =
          StandardWorkspace()
            .withWorkspaceId(workspaceId)
        whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace)

        val catalog = generateBasicApiCatalog()

        val destinationCatalogId = UUID.randomUUID()
        val connectionCreate =
          buildConnectionCreateRequest(standardSync, catalog)
            .destinationCatalogId(destinationCatalogId)

        val actualConnectionRead = connectionsHandler.createConnection(connectionCreate)

        val expectedConnectionRead = generateExpectedConnectionRead(standardSync)
        assertEquals(expectedConnectionRead, actualConnectionRead)

        standardSync.withDestinationCatalogId(destinationCatalogId)
        verify(connectionService).writeStandardSync(standardSync.withNotifySchemaChangesByEmail(null))
      }

      @ParameterizedTest
      @CsvSource(
        "true, true, true", // all supported
        "true, true, false", // stream is not file based
        "true, false, false", // destination does not support
        "false, true, false", // source does not support
        "false, false, false", // nothing supports it
      )
      fun testCreateConnectionValidatesFileTransfer(
        sourceSupportsFiles: Boolean,
        destinationSupportsFiles: Boolean,
        streamSupportsFiles: Boolean,
      ) {
        val workspace =
          StandardWorkspace()
            .withWorkspaceId(workspaceId)
        whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace)

        val catalog = generateBasicApiCatalog()
        catalog
          .streams
          .first()
          .stream
          .isFileBased(streamSupportsFiles)
        catalog
          .streams
          .first()
          .config
          .includeFiles(true)

        val connectionCreate = buildConnectionCreateRequest(standardSync, catalog)

        whenever(
          actorDefinitionVersionHelper.getSourceVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(
          ActorDefinitionVersion().withSupportsFileTransfer(sourceSupportsFiles),
        )
        whenever(
          actorDefinitionVersionHelper.getDestinationVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(
          ActorDefinitionVersion().withSupportsFileTransfer(destinationSupportsFiles),
        )

        if (sourceSupportsFiles && destinationSupportsFiles && streamSupportsFiles) {
          // everything supports file transfers, so the connection should be created successfully
          val actualRead = connectionsHandler.createConnection(connectionCreate)
          val expectedRead = generateExpectedConnectionRead(standardSync)
          assertEquals(expectedRead, actualRead)
        } else if (!sourceSupportsFiles || !destinationSupportsFiles) {
          // source or destination does not support file transfers
          assertThrows(
            ConnectionDoesNotSupportFileTransfersProblem::class.java,
          ) { connectionsHandler.createConnection(connectionCreate) }
        } else {
          // stream is not file based
          assertThrows(
            StreamDoesNotSupportFileTransfersProblem::class.java,
          ) { connectionsHandler.createConnection(connectionCreate) }
        }
      }

      @Test
      fun testCreateConnectionValidatesMappers() {
        val workspace =
          StandardWorkspace()
            .withWorkspaceId(workspaceId)
        whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
          .thenReturn(workspace)

        val catalog = generateBasicApiCatalog()

        val connectionCreate = buildConnectionCreateRequest(standardSync, catalog)

        val streamName = "stream-name"
        whenever(
          destinationCatalogGenerator.generateDestinationCatalog(
            catalogConverter.toConfiguredInternal(
              catalog,
            ),
          ),
        ).thenReturn(
          CatalogGenerationResult(
            ConfiguredAirbyteCatalog(),
            mapOf(
              StreamDescriptor().withName(streamName) to
                mapOf(
                  object : MapperConfig {
                    override fun name(): String = MapperOperationName.HASHING

                    override fun id(): UUID? = null

                    override fun documentationUrl(): String? = null

                    override fun config(): Any = mapOf<Any?, Any?>()
                  } to MapperError(MapperErrorType.INVALID_MAPPER_CONFIG, "error message"),
                ),
            ),
          ),
        )

        val exception: MapperValidationProblem =
          assertThrows(
            MapperValidationProblem::class.java,
          ) { connectionsHandler.createConnection(connectionCreate) }
        val problem = exception.problem as MapperValidationProblemResponse
        assertEquals(problem.getData()!!.errors.size, 1)
        assertEquals(
          problem.getData()!!.errors.first(),
          ProblemMapperErrorData()
            .stream(streamName)
            .error(MapperErrorType.INVALID_MAPPER_CONFIG.name)
            .mapper(ProblemMapperErrorDataMapper().type(MapperOperationName.HASHING).mapperConfiguration(mapOf<Any?, Any?>())),
        )
      }

      @Test
      fun testCreateFullRefreshConnectionWithSelectedFields() {
        val workspace =
          StandardWorkspace()
            .withWorkspaceId(workspaceId)
        whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace)

        val fullRefreshCatalogWithSelectedFields = generateApiCatalogWithTwoFields()
        fullRefreshCatalogWithSelectedFields
          .streams
          .get(0)
          .config
          .fieldSelectionEnabled(true)
          .selectedFields(listOf<@Valid SelectedFieldInfo?>(SelectedFieldInfo().addFieldPathItem(FIELD_NAME)))
          .cursorField(null)
          .syncMode(io.airbyte.api.model.generated.SyncMode.FULL_REFRESH)

        val connectionCreate = buildConnectionCreateRequest(standardSync, fullRefreshCatalogWithSelectedFields)

        val actualConnectionRead = connectionsHandler.createConnection(connectionCreate)

        val expectedConnectionRead = generateExpectedConnectionRead(standardSync)

        assertEquals(expectedConnectionRead, actualConnectionRead)

        standardSync
          .withFieldSelectionData(FieldSelectionData().withAdditionalProperty(streamSelectionData, true))
          .catalog
          .streams
          .get(0)
          .withSyncMode(SyncMode.FULL_REFRESH)
          .withCursorField(null)

        verify(connectionService).writeStandardSync(standardSync.withNotifySchemaChangesByEmail(null))
      }

      @Test
      fun testFieldSelectionRemoveCursorFails() {
        // Test that if we try to de-select a field that's being used for the cursor, the request will fail.
        // The connection initially has a catalog with one stream, and two fields in that stream.
        standardSync.setCatalog(generateAirbyteCatalogWithTwoFields())

        // Send an update that sets a cursor but de-selects that field.
        val catalogForUpdate = generateApiCatalogWithTwoFields()
        catalogForUpdate
          .streams
          .get(0)
          .config
          .fieldSelectionEnabled(true)
          .selectedFields(listOf<@Valid SelectedFieldInfo?>(SelectedFieldInfo().addFieldPathItem(FIELD_NAME)))
          .cursorField(listOf(SECOND_FIELD_NAME))
          .syncMode(io.airbyte.api.model.generated.SyncMode.INCREMENTAL)

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalogForUpdate)

        assertThrows(
          JsonValidationException::class.java,
        ) { connectionsHandler.updateConnection(connectionUpdate, null, false) }
      }

      @Test
      fun testFieldSelectionRemovePrimaryKeyFails() {
        // Test that if we try to de-select a field that's being used for the primary key, the request will
        // fail.
        // The connection initially has a catalog with one stream, and two fields in that stream.
        standardSync.setCatalog(generateAirbyteCatalogWithTwoFields())

        // Send an update that sets a primary key but deselects that field.
        val catalogForUpdate = generateApiCatalogWithTwoFields()
        catalogForUpdate
          .streams
          .get(0)
          .config
          .fieldSelectionEnabled(true)
          .selectedFields(listOf<@Valid SelectedFieldInfo?>(SelectedFieldInfo().addFieldPathItem(FIELD_NAME)))
          .destinationSyncMode(DestinationSyncMode.APPEND_DEDUP)
          .primaryKey(listOf(listOf(SECOND_FIELD_NAME)))

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalogForUpdate)

        assertThrows(
          JsonValidationException::class.java,
        ) { connectionsHandler.updateConnection(connectionUpdate, null, false) }
      }

      @Test
      fun testValidateConnectionCreateSourceAndDestinationInDifferenceWorkspace() {
        whenever(workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(destinationId)).thenReturn(UUID.randomUUID())

        val connectionCreate =
          ConnectionCreate()
            .sourceId(standardSync.sourceId)
            .destinationId(standardSync.destinationId)

        assertThrows(
          IllegalArgumentException::class.java,
        ) { connectionsHandler.createConnection(connectionCreate) }
      }

      @Test
      fun testCreateConnectionWithConflictingStreamsThrows() {
        whenever(featureFlagClient.boolVariation(ValidateConflictingDestinationStreams, Organization(organizationId)))
          .thenReturn(true)

        val catalog = generateBasicApiCatalog()

        val streamName: String? =
          catalog
            .streams
            .first()
            .stream
            .name
        val streamNamespace: String? =
          catalog
            .streams
            .first()
            .stream
            .namespace

        val mockDestinationStreamData =
          listOf(
            StreamDescriptorForDestination()
              .withStreamName(streamName)
              .withStreamNamespace(streamNamespace)
              .withNamespaceFormat(standardSync.namespaceFormat)
              .withNamespaceDefinition(standardSync.namespaceDefinition)
              .withPrefix(standardSync.prefix),
          )

        whenever(
          connectionService.listStreamsForDestination(
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(mockDestinationStreamData)

        val connectionCreate = buildConnectionCreateRequest(standardSync, catalog)

        assertThrows(
          ConnectionConflictingStreamProblem::class.java,
        ) { connectionsHandler.createConnection(connectionCreate) }
      }

      @Test
      fun testCreateConnectionWithNoConflictingStreamSucceeds() {
        whenever(featureFlagClient.boolVariation(ValidateConflictingDestinationStreams, Organization(organizationId)))
          .thenReturn(true)

        val catalog = generateBasicApiCatalog()

        whenever(
          connectionService.listStreamsForDestination(
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(mutableListOf())

        val connectionCreate = buildConnectionCreateRequest(standardSync, catalog)

        assertDoesNotThrow<ConnectionRead?> { connectionsHandler.createConnection(connectionCreate) }
      }

      @Test
      fun testCreateConnectionWithConflictingStreamButUnselected() {
        whenever(featureFlagClient.boolVariation(ValidateConflictingDestinationStreams, Organization(organizationId)))
          .thenReturn(true)

        val catalog = generateBasicApiCatalog()
        catalog
          .streams
          .first()
          .config
          .selected(false)

        val streamName: String? =
          catalog
            .streams
            .first()
            .stream
            .name
        val streamNamespace: String? =
          catalog
            .streams
            .first()
            .stream
            .namespace

        val mockDestinationStreamData =
          listOf(
            StreamDescriptorForDestination()
              .withStreamName(streamName)
              .withStreamNamespace(streamNamespace)
              .withNamespaceFormat(standardSync.namespaceFormat)
              .withNamespaceDefinition(standardSync.namespaceDefinition)
              .withPrefix(standardSync.prefix),
          )

        whenever(
          connectionService.listStreamsForDestination(
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(mockDestinationStreamData)

        val connectionCreate = buildConnectionCreateRequest(standardSync, catalog)

        assertDoesNotThrow<ConnectionRead?> { connectionsHandler.createConnection(connectionCreate) }
      }

      @Test
      fun testValidateConnectionCreateOperationInDifferentWorkspace() {
        whenever(workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(operationId)).thenReturn(UUID.randomUUID())

        val connectionCreate =
          ConnectionCreate()
            .sourceId(standardSync.sourceId)
            .destinationId(standardSync.destinationId)
            .operationIds(mutableListOf(operationId))

        assertThrows(
          IllegalArgumentException::class.java,
        ) { connectionsHandler.createConnection(connectionCreate) }
      }

      @Test
      fun testCreateConnectionWithBadDefinitionIds() {
        val sourceIdBad = UUID.randomUUID()
        val destinationIdBad = UUID.randomUUID()

        whenever(sourceService.getSourceConnection(sourceIdBad))
          .thenAnswer { throw ConfigNotFoundException(ConfigNotFoundType.SOURCE_CONNECTION, sourceIdBad) }
        whenever(destinationService.getDestinationConnection(destinationIdBad))
          .thenAnswer { throw ConfigNotFoundException(ConfigNotFoundType.DESTINATION_CONNECTION, destinationIdBad) }

        val catalog = generateBasicApiCatalog()

        val connectionCreateBadSource =
          ConnectionCreate()
            .sourceId(sourceIdBad)
            .destinationId(standardSync.destinationId)
            .operationIds(standardSync.operationIds)
            .name(prestoToHudi)
            .namespaceDefinition(NamespaceDefinitionType.SOURCE)
            .namespaceFormat(null)
            .prefix(prestoToHudiPrefix)
            .status(ConnectionStatus.ACTIVE)
            .schedule(generateBasicConnectionSchedule())
            .syncCatalog(catalog)

        assertThrows(
          ConfigNotFoundException::class.java,
        ) { connectionsHandler.createConnection(connectionCreateBadSource) }

        val connectionCreateBadDestination =
          ConnectionCreate()
            .sourceId(standardSync.sourceId)
            .destinationId(destinationIdBad)
            .operationIds(standardSync.operationIds)
            .name(prestoToHudi)
            .namespaceDefinition(NamespaceDefinitionType.SOURCE)
            .namespaceFormat(null)
            .prefix(prestoToHudiPrefix)
            .status(ConnectionStatus.ACTIVE)
            .schedule(generateBasicConnectionSchedule())
            .syncCatalog(catalog)
            .tags(mutableListOf<@Valid Tag?>())

        assertThrows(
          ConfigNotFoundException::class.java,
        ) { connectionsHandler.createConnection(connectionCreateBadDestination) }
      }

      @Test
      fun throwsBadRequestExceptionOnCatalogSizeValidationError() {
        val catalog = generateBasicApiCatalog()
        val request = buildConnectionCreateRequest(standardSync, catalog)
        whenever<ValidationError?>(
          catalogValidator.fieldCount(
            eq(catalog),
            anyOrNull(),
          ),
        ).thenReturn(
          ValidationError("bad catalog"),
        )

        assertThrows(
          BadRequestException::class.java,
        ) { connectionsHandler.createConnection(request) }
      }

      @ParameterizedTest
      @CsvSource(
        "true, true, true, true", // feature flag enabled, destination supports data activation, has destination catalog
        "true, true, false, false", // feature flag enabled, destination supports data activation, no destination catalog - should throw
        "true, false, false, true", // feature flag enabled, destination doesn't support data activation, no destination catalog
        "false, true, true, true", // feature flag disabled, destination supports data activation, has destination catalog
        "false, true, false, true", // feature flag disabled, destination supports data activation, no destination catalog
        "false, false, false, true", // feature flag disabled, destination doesn't support data activation, no destination catalog
      )
      fun testCreateConnectionValidatesDestinationCatalog(
        featureFlagEnabled: Boolean,
        destinationSupportsDataActivation: Boolean,
        hasDestinationCatalog: Boolean,
        shouldSucceed: Boolean,
      ) {
        val destinationCatalogIdInCreate = if (hasDestinationCatalog) UUID.randomUUID() else null

        val catalogForCreate = generateBasicApiCatalog()
        val connectionCreate = buildConnectionCreateRequest(standardSync, catalogForCreate)
        if (hasDestinationCatalog) {
          connectionCreate.destinationCatalogId(destinationCatalogIdInCreate)
          whenever(catalogService.getActorCatalogById(destinationCatalogIdInCreate!!))
            .thenReturn(ActorCatalog().withId(destinationCatalogIdInCreate).withCatalog(emptyObject()))
          whenever(connectionService.getStandardSync(standardSync.connectionId))
            .thenReturn(clone(standardSync).withDestinationCatalogId(destinationCatalogIdInCreate))
        }

        whenever(featureFlagClient.boolVariation(EnableDestinationCatalogValidation, Workspace(workspaceId)))
          .thenReturn(featureFlagEnabled)
        whenever(
          actorDefinitionVersionHelper.getSourceVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(
          ActorDefinitionVersion().withSupportsDataActivation(false),
        )
        whenever(
          actorDefinitionVersionHelper.getDestinationVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(
          ActorDefinitionVersion().withSupportsDataActivation(destinationSupportsDataActivation),
        )

        if (shouldSucceed) {
          // Connection should be created successfully
          val actualRead = connectionsHandler.createConnection(connectionCreate)
          val expectedRead =
            generateExpectedConnectionRead(standardSync)
              .syncCatalog(catalogForCreate)
              .destinationCatalogId(destinationCatalogIdInCreate)
          assertEquals(expectedRead, actualRead)
        } else {
          // Should throw DestinationCatalogRequiredProblem
          assertThrows(
            DestinationCatalogRequiredProblem::class.java,
          ) { connectionsHandler.createConnection(connectionCreate) }
        }
      }
    }

    @Nested
    internal inner class UpdateConnection {
      var moreComplexCatalogSync: StandardSync? = null

      var complexConfiguredCatalog: ConfiguredAirbyteCatalog? = null

      var complexCatalog: AirbyteCatalog? = null

      val catalogStreamNames: MutableList<String?> = mutableListOf("user", "permission", "organization", "workspace", "order")

      @BeforeEach
      fun setup() {
        val connection3Id = UUID.randomUUID()
        whenever(workspaceHelper.getWorkspaceForDestinationId(anyOrNull())).thenReturn(workspaceId)
        whenever(workspaceHelper.getWorkspaceForConnectionId(standardSync.connectionId)).thenReturn(workspaceId)
        whenever(workspaceHelper.getOrganizationForWorkspace(anyOrNull())).thenReturn(organizationId)

        complexConfiguredCatalog =
          ConfiguredAirbyteCatalog()
            .withStreams(
              catalogStreamNames
                .stream()
                .map { name: String? -> this.buildConfiguredStream(name!!) }
                .toList(),
            )

        complexCatalog =
          AirbyteCatalog().streams(
            catalogStreamNames.stream().map<AirbyteStreamAndConfiguration?> { name: String? -> this.buildStream(name) }.toList(),
          )

        moreComplexCatalogSync =
          StandardSync()
            .withConnectionId(connection3Id)
            .withName("Connection with non trivial catalog")
            .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
            .withNamespaceFormat(null)
            .withPrefix("none")
            .withStatus(StandardSync.Status.ACTIVE)
            .withCatalog(complexConfiguredCatalog)
            .withSourceId(sourceId)
            .withDestinationId(destinationId)
            .withOperationIds(mutableListOf<UUID?>())
            .withManual(false)
            .withSchedule(generateBasicSchedule())
            .withScheduleType(StandardSync.ScheduleType.BASIC_SCHEDULE)
            .withScheduleData(generateBasicScheduleData())
            .withResourceRequirements(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS)
            .withSourceCatalogId(UUID.randomUUID())
            .withNotifySchemaChanges(false)
            .withNotifySchemaChangesByEmail(true)
            .withBreakingChange(false)
        whenever(connectionService.getStandardSync(moreComplexCatalogSync!!.connectionId))
          .thenReturn(moreComplexCatalogSync)
        whenever(workspaceHelper.getWorkspaceForConnectionId(connection3Id)).thenReturn(workspaceId)
        whenever(sourceService.getSourceDefinitionFromConnection(connection3Id))
          .thenReturn(StandardSourceDefinition().withName("source").withSourceDefinitionId(UUID.randomUUID()))
        whenever(destinationService.getDestinationDefinitionFromConnection(connection3Id))
          .thenReturn(StandardDestinationDefinition().withName("destination").withDestinationDefinitionId(UUID.randomUUID()))
        val sourceVersion = mock(ActorDefinitionVersion::class.java)
        val destinationVersion = mock(ActorDefinitionVersion::class.java)
        whenever(sourceVersion.supportsFileTransfer).thenReturn(false)
        whenever(destinationVersion.supportsFileTransfer).thenReturn(false)
        whenever(
          actorDefinitionVersionHelper.getSourceVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(sourceVersion)
        whenever(
          actorDefinitionVersionHelper.getDestinationVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(destinationVersion)
      }

      private fun buildConfiguredStream(name: String): ConfiguredAirbyteStream =
        ConfiguredAirbyteStream(
          createAirbyteStream(name, Field.of(FIELD_NAME, JsonSchemaType.STRING))
            .withDefaultCursorField(listOf(FIELD_NAME))
            .withSourceDefinedCursor(false)
            .withSupportedSyncModes(
              listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
            ),
          SyncMode.INCREMENTAL,
          io.airbyte.config.DestinationSyncMode.APPEND,
        ).withCursorField(listOf(FIELD_NAME))

      private fun buildStream(name: String?): AirbyteStreamAndConfiguration? =
        AirbyteStreamAndConfiguration()
          .stream(
            AirbyteStream()
              .name(name)
              .jsonSchema(emptyObject())
              .supportedSyncModes(listOf(io.airbyte.api.model.generated.SyncMode.INCREMENTAL)),
          ).config(
            AirbyteStreamConfiguration()
              .syncMode(io.airbyte.api.model.generated.SyncMode.INCREMENTAL)
              .destinationSyncMode(
                DestinationSyncMode.APPEND,
              ).selected(true),
          )

      @Test
      fun testUpdateConnectionLockedThrowsProblem() {
        val lockedSync =
          clone(
            standardSync,
          ).withStatus(io.airbyte.config.StandardSync.Status.LOCKED).withStatusReason(StatusReason.SUBSCRIPTION_DOWNGRADED_ACCESS_REVOKED.value)
        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(lockedSync)

        val connectionUpdate = ConnectionUpdate().connectionId(standardSync.connectionId).name("newName")

        assertThrows(
          ConnectionLockedProblem::class.java,
        ) { connectionsHandler.updateConnection(connectionUpdate, null, false) }
      }

      @Test
      fun testUpdateConnectionPatchSingleField() {
        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .name("newName")

        val expectedRead =
          generateExpectedConnectionRead(standardSync)
            .name("newName")
        val expectedPersistedSync = clone(standardSync).withName("newName")

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        assertEquals(expectedRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @Test
      fun testUpdateConnectionPatchScheduleToManual() {
        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .scheduleType(ConnectionScheduleType.MANUAL)

        val expectedRead =
          generateExpectedConnectionRead(standardSync)
            .schedule(null)
            .scheduleType(ConnectionScheduleType.MANUAL)
            .scheduleData(null)

        val expectedPersistedSync =
          clone(standardSync)
            .withSchedule(null)
            .withScheduleType(StandardSync.ScheduleType.MANUAL)
            .withScheduleData(null)
            .withManual(true)

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        assertEquals(expectedRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @Test
      fun testUpdateConnectionWithDuplicateStreamsShouldThrowException() {
        val catalog = generateMultipleStreamsApiCatalog(false, false, 2)

        val connectionCreate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalog)

        assertThrows(
          IllegalArgumentException::class.java,
        ) { connectionsHandler.updateConnection(connectionCreate, null, false) }
      }

      @ParameterizedTest
      @ValueSource(strings = ["SOURCE_CONNECTOR", "DESTINATION_CONNECTOR"])
      fun testUpdateConnectionWithUnentitledConnectorThrows(entitlement: Entitlement) {
        val catalog = generateBasicApiCatalog()

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalog)

        doThrow(LicenseEntitlementProblem())
          .`when`(licenseEntitlementChecker)
          .ensureEntitled(anyOrNull(), eq(entitlement), anyOrNull())

        assertThrows(
          LicenseEntitlementProblem::class.java,
        ) { connectionsHandler.updateConnection(connectionUpdate, null, false) }
      }

      @Test
      fun testUpdateConnectionPatchScheduleToCron() {
        whenever(workspaceHelper.getWorkspaceForSourceId(anyOrNull())).thenReturn(UUID.randomUUID())
        whenever(workspaceHelper.getOrganizationForWorkspace(anyOrNull())).thenReturn(organizationId)

        val cronScheduleData =
          ConnectionScheduleData().cron(
            ConnectionScheduleDataCron().cronExpression(cronExpression).cronTimeZone(cronTimezoneUtc),
          )

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .scheduleType(ConnectionScheduleType.CRON)
            .scheduleData(cronScheduleData)

        val expectedRead =
          generateExpectedConnectionRead(standardSync)
            .schedule(null)
            .scheduleType(ConnectionScheduleType.CRON)
            .scheduleData(cronScheduleData)

        val expectedPersistedSync =
          clone(standardSync)
            .withSchedule(null)
            .withScheduleType(StandardSync.ScheduleType.CRON)
            .withScheduleData(ScheduleData().withCron(Cron().withCronExpression(cronExpression).withCronTimeZone(cronTimezoneUtc)))
            .withManual(false)

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)
        whenever(
          entitlementService.checkEntitlement(
            OrganizationId(organizationId),
            FasterSyncFrequencyEntitlement,
          ),
        ).thenReturn(EntitlementResult(FasterSyncFrequencyEntitlement.featureId, true, null))

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        assertEquals(expectedRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @Test
      fun testUpdateConnectionPatchBasicSchedule() {
        val newScheduleData =
          ConnectionScheduleData().basicSchedule(
            ConnectionScheduleDataBasicSchedule().timeUnit(ConnectionScheduleDataBasicSchedule.TimeUnitEnum.DAYS).units(10L),
          )

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .scheduleType(ConnectionScheduleType.BASIC) // update route requires this to be set even if it isn't changing
            .scheduleData(newScheduleData)

        val expectedRead =
          generateExpectedConnectionRead(standardSync)
            .schedule(ConnectionSchedule().timeUnit(ConnectionSchedule.TimeUnitEnum.DAYS).units(10L)) // still dual-writing to legacy field
            .scheduleType(ConnectionScheduleType.BASIC)
            .scheduleData(newScheduleData)

        val expectedPersistedSync =
          clone(standardSync)
            .withSchedule(Schedule().withTimeUnit(Schedule.TimeUnit.DAYS).withUnits(10L)) // still dual-writing to legacy field
            .withScheduleType(StandardSync.ScheduleType.BASIC_SCHEDULE)
            .withScheduleData(ScheduleData().withBasicSchedule(BasicSchedule().withTimeUnit(BasicSchedule.TimeUnit.DAYS).withUnits(10L)))
            .withManual(false)

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        assertEquals(expectedRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @Test
      fun testUpdateConnectionPatchAddingNewStream() {
        // the connection initially has a catalog with one stream. this test generates another catalog with
        // one stream, changes that stream's name to something new, and sends both streams in the patch
        // request.
        // the test expects the final result to include both streams.
        val catalogWithNewStream = generateBasicApiCatalog()
        catalogWithNewStream
          .streams
          .get(0)
          .stream.name = azkabanUsers
        catalogWithNewStream
          .streams
          .get(0)
          .config.aliasName = azkabanUsers

        val catalogForUpdate = generateMultipleStreamsApiCatalog(2)
        catalogForUpdate
          .streams
          .get(1)
          .stream.name = azkabanUsers
        catalogForUpdate
          .streams
          .get(1)
          .config.aliasName = azkabanUsers

        // expect two streams in the final persisted catalog -- the original unchanged stream, plus the new
        // azkabanUsers stream
        val expectedPersistedCatalog = generateMultipleStreamsConfiguredAirbyteCatalog(2)
        expectedPersistedCatalog.streams
          .get(1)
          .stream.name = azkabanUsers

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalogForUpdate)

        val expectedRead =
          generateExpectedConnectionRead(standardSync)
            .syncCatalog(catalogForUpdate)

        val expectedPersistedSync =
          clone(standardSync)
            .withCatalog(expectedPersistedCatalog)
            .withFieldSelectionData(catalogConverter.getFieldSelectionData(catalogForUpdate))

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        assertEquals(expectedRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @Test
      fun testUpdateConnectionPatchEditExistingStreamWhileAddingNewStream() {
        // the connection initially has a catalog with two streams. this test updates the catalog
        // with a sync mode change for one of the initial streams while also adding a brand-new
        // stream. The result should be a catalog with three streams.
        standardSync.catalog = generateMultipleStreamsConfiguredAirbyteCatalog(2)

        val catalogForUpdate = generateMultipleStreamsApiCatalog(3)
        catalogForUpdate
          .streams
          .get(0)
          .config.syncMode = io.airbyte.api.model.generated.SyncMode.FULL_REFRESH
        catalogForUpdate
          .streams
          .get(2)
          .stream.name = azkabanUsers
        catalogForUpdate
          .streams
          .get(2)
          .config.aliasName = azkabanUsers

        // expect three streams in the final persisted catalog
        val expectedPersistedCatalog = generateMultipleStreamsConfiguredAirbyteCatalog(3)
        expectedPersistedCatalog.streams[0].withSyncMode(SyncMode.FULL_REFRESH)
        // index 1 is unchanged
        expectedPersistedCatalog.streams
          .get(2)
          .stream
          .withName(azkabanUsers)

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalogForUpdate)

        val expectedRead =
          generateExpectedConnectionRead(standardSync)
            .syncCatalog(catalogForUpdate)

        val expectedPersistedSync =
          clone(standardSync)
            .withCatalog(expectedPersistedCatalog)
            .withFieldSelectionData(catalogConverter.getFieldSelectionData(catalogForUpdate))

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        assertEquals(expectedRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @Test
      fun testUpdateConnectionPatchDestinationCatalogId() {
        val newDestinationCatalogId = UUID.randomUUID()
        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .destinationCatalogId(newDestinationCatalogId)

        val expectedRead =
          generateExpectedConnectionRead(standardSync)
            .destinationCatalogId(newDestinationCatalogId)

        val expectedPersistedSync =
          clone(standardSync)
            .withDestinationCatalogId(newDestinationCatalogId)

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        assertEquals(expectedRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @ParameterizedTest
      @CsvSource(
        "true, true, true", // all supported
        "true, true, false", // stream is not file based
        "true, false, false", // destination does not support
        "false, true, false", // source does not support
        "false, false, false", // nothing supports it
      )
      fun testUpdateConnectionValidatesFileTransfer(
        sourceSupportsFiles: Boolean,
        destinationSupportsFiles: Boolean,
        streamSupportsFiles: Boolean,
      ) {
        val catalogForUpdate = generateBasicApiCatalog()
        catalogForUpdate
          .streams
          .first()
          .stream
          .isFileBased(streamSupportsFiles)
        catalogForUpdate
          .streams
          .first()
          .config
          .setIncludeFiles(true)

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalogForUpdate)

        whenever(
          actorDefinitionVersionHelper.getSourceVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(
          ActorDefinitionVersion().withSupportsFileTransfer(sourceSupportsFiles),
        )
        whenever(
          actorDefinitionVersionHelper.getDestinationVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(
          ActorDefinitionVersion().withSupportsFileTransfer(destinationSupportsFiles),
        )

        if (sourceSupportsFiles && destinationSupportsFiles && streamSupportsFiles) {
          // everything supports file transfers, so the connection should be created successfully
          val actualRead = connectionsHandler.updateConnection(connectionUpdate, null, false)
          val expectedRead = generateExpectedConnectionRead(standardSync).syncCatalog(catalogForUpdate)
          assertEquals(expectedRead, actualRead)
        } else if (!sourceSupportsFiles || !destinationSupportsFiles) {
          // source or destination does not support file transfers
          assertThrows(
            ConnectionDoesNotSupportFileTransfersProblem::class.java,
          ) { connectionsHandler.updateConnection(connectionUpdate, null, false) }
        } else {
          // stream is not file based
          assertThrows(
            StreamDoesNotSupportFileTransfersProblem::class.java,
          ) { connectionsHandler.updateConnection(connectionUpdate, null, false) }
        }
      }

      @Test
      fun testUpdateConnectionPatchValidatesMappers() {
        standardSync.setCatalog(generateAirbyteCatalogWithTwoFields())

        val catalogForUpdate = generateApiCatalogWithTwoFields()
        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalogForUpdate)

        val streamName = "stream-name"
        val mapperId = UUID.randomUUID()
        whenever(connectionService.getStandardSync(standardSync.connectionId))
          .thenReturn(standardSync)
        whenever(
          destinationCatalogGenerator.generateDestinationCatalog(
            catalogConverter.toConfiguredInternal(
              catalogForUpdate,
            ),
          ),
        ).thenReturn(
          CatalogGenerationResult(
            ConfiguredAirbyteCatalog(),
            mapOf(
              StreamDescriptor().withName(streamName) to
                mapOf(
                  object : MapperConfig {
                    override fun config(): Any = mapOf<Any?, Any?>()

                    override fun documentationUrl(): String? = null

                    override fun id(): UUID = mapperId

                    override fun name(): String = MapperOperationName.HASHING
                  } to MapperError(MapperErrorType.INVALID_MAPPER_CONFIG, "error"),
                ),
            ),
          ),
        )

        val exception: MapperValidationProblem =
          assertThrows(
            MapperValidationProblem::class.java,
          ) { connectionsHandler.updateConnection(connectionUpdate, null, false) }
        val problem = exception.problem as MapperValidationProblemResponse
        assertEquals(problem.getData()!!.getErrors().size, 1)
        assertEquals(
          problem.getData()!!.getErrors().first(),
          ProblemMapperErrorData()
            .stream(streamName)
            .error(MapperErrorType.INVALID_MAPPER_CONFIG.name)
            .mapper(
              ProblemMapperErrorDataMapper().id(mapperId).type(MapperOperationName.HASHING).mapperConfiguration(mapOf<Any?, Any?>()),
            ),
        )
      }

      @Test
      fun testUpdateConnectionPatchHashedFields() {
        // The connection initially has a catalog with one stream, and two fields in that stream.
        standardSync.catalog = generateAirbyteCatalogWithTwoFields()

        // Send an update that hashes one of the fields
        val catalogForUpdate = generateApiCatalogWithTwoFields()
        catalogForUpdate
          .streams
          .first()
          .config
          .hashedFields(listOf<@Valid SelectedFieldInfo?>(SelectedFieldInfo().addFieldPathItem(FIELD_NAME)))

        // Expect mapper in the persisted catalog
        val hashingMapper = createHashingMapper(FIELD_NAME)
        val expectedPersistedCatalog = generateAirbyteCatalogWithTwoFields()
        expectedPersistedCatalog.streams.first().mappers = listOf(hashingMapper)

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalogForUpdate)

        // Ensure mappers are populated as well
        val expectedCatalog = clone(catalogForUpdate)
        expectedCatalog.streams.first().config.addMappersItem(
          ConfiguredStreamMapper().type(StreamMapperType.HASHING).mapperConfiguration(jsonNode<HashingConfig?>(hashingMapper.config)),
        )

        val expectedRead =
          generateExpectedConnectionRead(standardSync)
            .syncCatalog(expectedCatalog)

        val expectedPersistedSync =
          clone(standardSync)
            .withCatalog(expectedPersistedCatalog)

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        assertEquals(expectedRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @Test
      fun testUpdateConnectionPatchMappers() {
        // The connection initially has a catalog with one stream, and two fields in that stream.
        standardSync.setCatalog(generateAirbyteCatalogWithTwoFields())

        // Send an update that hashes one of the fields, using mappers
        val hashingMapper = createHashingMapper(FIELD_NAME, UUID.randomUUID())
        val catalogForUpdate = generateApiCatalogWithTwoFields()
        catalogForUpdate.streams.first().config.addMappersItem(
          ConfiguredStreamMapper()
            .id(hashingMapper.id())
            .type(StreamMapperType.HASHING)
            .mapperConfiguration(jsonNode<HashingConfig?>(hashingMapper.config)),
        )

        // Expect mapper in the persisted catalog
        val expectedPersistedCatalog = generateAirbyteCatalogWithTwoFields()
        expectedPersistedCatalog.streams.first().mappers = listOf(hashingMapper)

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalogForUpdate)

        // Ensure hashedFields is set as well for backwards-compatibility with UI expectations
        val expectedCatalog = clone(catalogForUpdate)
        expectedCatalog
          .streams
          .first()
          .config
          .addHashedFieldsItem(SelectedFieldInfo().addFieldPathItem(FIELD_NAME))

        val expectedRead =
          generateExpectedConnectionRead(standardSync)
            .syncCatalog(expectedCatalog)

        val expectedPersistedSync =
          clone(standardSync)
            .withCatalog(expectedPersistedCatalog)

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        assertEquals(expectedRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @Test
      fun testUpdateConnectionPatchColumnSelection() {
        // The connection initially has a catalog with one stream, and two fields in that stream.
        standardSync.setCatalog(generateAirbyteCatalogWithTwoFields())

        // Send an update that only selects one of the fields.
        val catalogForUpdate = generateApiCatalogWithTwoFields()
        catalogForUpdate
          .streams
          .get(0)
          .config
          .fieldSelectionEnabled(true)
          .selectedFields(listOf<@Valid SelectedFieldInfo?>(SelectedFieldInfo().addFieldPathItem(FIELD_NAME)))

        // Expect one column in the final persisted catalog
        val expectedPersistedCatalog = generateBasicConfiguredAirbyteCatalog()

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalogForUpdate)

        val expectedRead =
          generateExpectedConnectionRead(standardSync)
            .syncCatalog(catalogForUpdate)

        val expectedPersistedSync =
          clone(standardSync)
            .withCatalog(expectedPersistedCatalog)
            .withFieldSelectionData(catalogConverter.getFieldSelectionData(catalogForUpdate))

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        assertEquals(expectedRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @Test
      fun testUpdateConnectionPatchingSeveralFieldsAndReplaceAStream() {
        val catalogForUpdate = generateMultipleStreamsApiCatalog(2)

        // deselect the existing stream, and add a new stream called 'azkaban_users'.
        // result that we persist and read after update should be a catalog with a single
        // stream called 'azkaban_users'.
        catalogForUpdate
          .streams
          .get(0)
          .config.selected = false
        catalogForUpdate
          .streams
          .get(1)
          .stream.name = azkabanUsers
        catalogForUpdate
          .streams
          .get(1)
          .config.aliasName = azkabanUsers

        val newSourceCatalogId = UUID.randomUUID()

        val resourceRequirements =
          ResourceRequirements()
            .cpuLimit(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.cpuLimit)
            .cpuRequest(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.cpuRequest)
            .memoryLimit(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.memoryLimit)
            .memoryRequest(ConnectionHelpers.TESTING_RESOURCE_REQUIREMENTS.memoryRequest)

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .status(ConnectionStatus.INACTIVE)
            .scheduleType(ConnectionScheduleType.MANUAL)
            .syncCatalog(catalogForUpdate)
            .resourceRequirements(resourceRequirements)
            .sourceCatalogId(newSourceCatalogId)
            .operationIds(listOf<UUID?>(operationId, otherOperationId))

        val expectedPersistedCatalog = generateBasicConfiguredAirbyteCatalog()
        expectedPersistedCatalog.streams
          .get(0)
          .stream
          .withName(azkabanUsers)

        val expectedPersistedSync =
          clone(standardSync)
            .withStatus(StandardSync.Status.INACTIVE)
            .withScheduleType(StandardSync.ScheduleType.MANUAL)
            .withScheduleData(null)
            .withSchedule(null)
            .withManual(true)
            .withCatalog(expectedPersistedCatalog)
            .withFieldSelectionData(catalogConverter.getFieldSelectionData(catalogForUpdate))
            .withResourceRequirements(apiPojoConverters.resourceRequirementsToInternal(resourceRequirements))
            .withSourceCatalogId(newSourceCatalogId)
            .withOperationIds(listOf<UUID?>(operationId, otherOperationId))

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        val expectedCatalogInRead = generateBasicApiCatalog()
        expectedCatalogInRead
          .streams
          .get(0)
          .stream.name = azkabanUsers
        expectedCatalogInRead
          .streams
          .get(0)
          .config.aliasName = azkabanUsers

        val expectedConnectionRead =
          generateExpectedConnectionRead(
            standardSync.connectionId,
            standardSync.sourceId,
            standardSync.destinationId,
            standardSync.operationIds,
            newSourceCatalogId,
            false,
            standardSync.getNotifySchemaChanges(),
            standardSync.getNotifySchemaChangesByEmail(),
            standardSync.getBackfillPreference().convertTo<SchemaChangeBackfillPreference>(),
            standardSync
              .tags
              .stream()
              .map { tag: io.airbyte.config.Tag? -> apiPojoConverters.toApiTag(tag!!) }
              .toList(),
          ).status(ConnectionStatus.INACTIVE)
            .scheduleType(ConnectionScheduleType.MANUAL)
            .scheduleData(null)
            .schedule(null)
            .syncCatalog(expectedCatalogInRead)
            .resourceRequirements(resourceRequirements)

        assertEquals(expectedConnectionRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @Test
      fun testUpdateConnectionPatchTags() {
        val workspaceId = UUID.randomUUID()
        val apiTag1 =
          Tag()
            .tagId(UUID.randomUUID())
            .workspaceId(workspaceId)
            .name("tag1")
            .color("ABC123")
        val apiTag2 =
          Tag()
            .tagId(UUID.randomUUID())
            .workspaceId(workspaceId)
            .name("tag2")
            .color("000000")
        val apiTag3 =
          Tag()
            .tagId(UUID.randomUUID())
            .workspaceId(workspaceId)
            .name("tag3")
            .color("FFFFFF")

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .tags(listOf<@Valid Tag?>(apiTag1, apiTag2, apiTag3))

        val expectedRead =
          generateExpectedConnectionRead(standardSync)
            .tags(listOf<@Valid Tag?>(apiTag1, apiTag2, apiTag3))

        val configTag1 = apiPojoConverters.toInternalTag(apiTag1)
        val configTag2 = apiPojoConverters.toInternalTag(apiTag2)
        val configTag3 = apiPojoConverters.toInternalTag(apiTag3)

        val expectedPersistedSync =
          clone(standardSync)
            .withTags(listOf(configTag1, configTag2, configTag3))

        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val actualConnectionRead = connectionsHandler.updateConnection(connectionUpdate, null, false)

        assertEquals(expectedRead, actualConnectionRead)
        verify(connectionService).writeStandardSync(expectedPersistedSync)
        verify(eventRunner).update(connectionUpdate.connectionId)
      }

      @Test
      fun testUpdateConnectionWithConflictingStreamsThrows() {
        whenever(featureFlagClient.boolVariation(ValidateConflictingDestinationStreams, Organization(organizationId)))
          .thenReturn(true)

        val catalog = generateBasicApiCatalog()

        val streamName: String? =
          catalog
            .streams
            .first()
            .stream
            .name
        val streamNamespace: String? =
          catalog
            .streams
            .first()
            .stream
            .namespace

        val mockDestinationStreamData =
          listOf(
            StreamDescriptorForDestination()
              .withStreamName(streamName)
              .withStreamNamespace(streamNamespace)
              .withNamespaceFormat(standardSync.namespaceFormat)
              .withNamespaceDefinition(standardSync.namespaceDefinition)
              .withPrefix(standardSync.prefix),
          )

        whenever(
          connectionService.listStreamsForDestination(
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(mockDestinationStreamData)

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalog)
        assertThrows(
          ConnectionConflictingStreamProblem::class.java,
        ) { connectionsHandler.updateConnection(connectionUpdate, null, false) }
      }

      @Test
      fun testUpdateConnectionWithNoConflictingStreamSucceeds() {
        whenever(featureFlagClient.boolVariation(ValidateConflictingDestinationStreams, Organization(organizationId)))
          .thenReturn(true)

        val catalog = generateBasicApiCatalog()

        whenever(
          connectionService.listStreamsForDestination(
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(mutableListOf())

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalog)

        assertDoesNotThrow<ConnectionRead?> {
          connectionsHandler.updateConnection(
            connectionUpdate,
            null,
            false,
          )
        }
      }

      @Test
      fun testUpdateConnectionWithConflictingStreamButUnselected() {
        whenever(featureFlagClient.boolVariation(ValidateConflictingDestinationStreams, Organization(organizationId)))
          .thenReturn(true)

        val catalog = generateBasicApiCatalog()
        catalog
          .streams
          .first()
          .config
          .selected(false)

        val streamName: String? =
          catalog
            .streams
            .first()
            .stream
            .name
        val streamNamespace: String? =
          catalog
            .streams
            .first()
            .stream
            .namespace

        val mockDestinationStreamData =
          listOf(
            StreamDescriptorForDestination()
              .withStreamName(streamName)
              .withStreamNamespace(streamNamespace)
              .withNamespaceFormat(standardSync.namespaceFormat)
              .withNamespaceDefinition(standardSync.namespaceDefinition)
              .withPrefix(standardSync.prefix),
          )

        whenever(
          connectionService.listStreamsForDestination(
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(mockDestinationStreamData)

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalog)

        assertDoesNotThrow<ConnectionRead?> {
          connectionsHandler.updateConnection(
            connectionUpdate,
            null,
            false,
          )
        }
      }

      @Test
      fun testValidateConnectionUpdateOperationInDifferentWorkspace() {
        whenever(workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(operationId)).thenReturn(UUID.randomUUID())
        whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .operationIds(mutableListOf(operationId))
            .syncCatalog(catalogConverter.toApi(standardSync.catalog, standardSync.fieldSelectionData))

        assertThrows(
          IllegalArgumentException::class.java,
        ) { connectionsHandler.updateConnection(connectionUpdate, null, false) }
      }

      @Test
      fun throwsBadRequestExceptionOnCatalogSizeValidationError() {
        val catalog = generateBasicApiCatalog()
        val request =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalog)
            .name("newName")
        whenever<ValidationError?>(
          catalogValidator.fieldCount(
            eq(catalog),
            anyOrNull(),
          ),
        ).thenReturn(
          ValidationError("bad catalog"),
        )

        assertThrows(
          BadRequestException::class.java,
        ) { connectionsHandler.updateConnection(request, null, false) }
      }

      @Test
      fun testDeactivateStreamsWipeState() {
        val catalog = complexCatalog!!
        val deactivatedStreams = mutableListOf<String?>("user", "permission")
        val stillActiveStreams = catalogStreamNames.stream().filter { s: String? -> !deactivatedStreams.contains(s) }.toList()

        catalog.setStreams(
          Stream
            .concat(
              stillActiveStreams.stream().map<AirbyteStreamAndConfiguration?> { name: String? -> this.buildStream(name) },
              deactivatedStreams
                .stream()
                .map<AirbyteStreamAndConfiguration?> { name: String? -> this.buildStream(name) }
                .peek { s: AirbyteStreamAndConfiguration? ->
                  s!!.setConfig(
                    AirbyteStreamConfiguration()
                      .syncMode(io.airbyte.api.model.generated.SyncMode.INCREMENTAL)
                      .destinationSyncMode(DestinationSyncMode.APPEND)
                      .selected(false),
                  )
                },
            ).toList(),
        )
        val request =
          ConnectionUpdate()
            .connectionId(moreComplexCatalogSync!!.connectionId)
            .syncCatalog(catalog)
        whenever(featureFlagClient.boolVariation(ResetStreamsStateWhenDisabled, Workspace(workspaceId))).thenReturn(true)
        connectionsHandler.updateConnection(request, null, false)
        val expectedStreams =
          setOf(StreamDescriptor().withName("user"), StreamDescriptor().withName("permission"))
        verify(statePersistence).bulkDelete(moreComplexCatalogSync!!.connectionId, expectedStreams)
      }

      @ParameterizedTest
      @CsvSource(
        "true, true, true, true", // feature flag enabled, destination supports data activation, has destination catalog
        "true, true, false, false", // feature flag enabled, destination supports data activation, no destination catalog - should throw
        "true, false, false, true", // feature flag enabled, destination doesn't support data activation, no destination catalog
        "false, true, true, true", // feature flag disabled, destination supports data activation, has destination catalog
        "false, true, false, true", // feature flag disabled, destination supports data activation, no destination catalog
        "false, false, false, true", // feature flag disabled, destination doesn't support data activation, no destination catalog
      )
      fun testUpdateConnectionValidatesDestinationCatalog(
        featureFlagEnabled: Boolean,
        destinationSupportsDataActivation: Boolean,
        hasDestinationCatalogInUpdate: Boolean,
        shouldSucceed: Boolean,
      ) {
        val catalogForUpdate = generateBasicApiCatalog()
        val connectionUpdate =
          ConnectionUpdate()
            .connectionId(standardSync.connectionId)
            .syncCatalog(catalogForUpdate)

        val destinationCatalogIdInUpdate = if (hasDestinationCatalogInUpdate) UUID.randomUUID() else null

        // Set destination catalog ID in the update if specified
        if (hasDestinationCatalogInUpdate) {
          connectionUpdate.destinationCatalogId(destinationCatalogIdInUpdate)
          whenever(catalogService.getActorCatalogById(destinationCatalogIdInUpdate!!))
            .thenReturn(ActorCatalog().withId(destinationCatalogIdInUpdate).withCatalog(emptyObject()))
        }

        whenever(
          featureFlagClient.boolVariation(
            eq(EnableDestinationCatalogValidation),
            anyOrNull(),
          ),
        ).thenReturn(featureFlagEnabled)
        whenever(
          actorDefinitionVersionHelper.getSourceVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(
          ActorDefinitionVersion().withSupportsDataActivation(false),
        )
        whenever(
          actorDefinitionVersionHelper.getDestinationVersion(
            anyOrNull(),
            anyOrNull(),
            anyOrNull(),
          ),
        ).thenReturn(
          ActorDefinitionVersion().withSupportsDataActivation(destinationSupportsDataActivation),
        )

        if (shouldSucceed) {
          // Connection should be updated successfully
          val actualRead = connectionsHandler.updateConnection(connectionUpdate, null, false)
          val expectedRead =
            generateExpectedConnectionRead(standardSync)
              .syncCatalog(catalogForUpdate)
              .destinationCatalogId(destinationCatalogIdInUpdate)
          assertEquals(expectedRead, actualRead)
        } else {
          // Should throw DestinationCatalogRequiredProblem
          assertThrows(DestinationCatalogRequiredProblem::class.java) {
            connectionsHandler.updateConnection(connectionUpdate, null, false)
          }
        }
      }
    }

    @Nested
    internal inner class ValidateCatalogWithDestinationCatalog {
      @Test
      fun testValidateCatalogWithDestinationCatalogSuccess() {
        // Create test data
        val catalog = generateBasicApiCatalog()
        catalog
          .streams
          .first()
          .config
          .destinationObjectName("test_table")

        val configuredCatalog = catalogConverter.toConfiguredInternal(catalog)

        val destinationSchema = generateBasicJsonSchema()
        (destinationSchema as ObjectNode).put("additionalProperties", false)

        val destinationCatalog =
          DestinationCatalog(
            listOf(
              DestinationOperation(
                "test_table",
                io.airbyte.config.DestinationSyncMode.APPEND,
                destinationSchema,
                null,
              ),
            ),
          )

        val generationResult = CatalogGenerationResult(configuredCatalog, mapOf<StreamDescriptor, MutableMap<MapperConfig, MapperError>>())

        whenever(
          destinationCatalogGenerator.generateDestinationCatalog(
            configuredCatalog,
          ),
        ).thenReturn(generationResult)

        // Should not throw any exception
        assertDoesNotThrow {
          connectionsHandler.validateCatalogWithDestinationCatalog(
            catalog,
            destinationCatalog,
          )
        }
      }

      @Test
      fun testValidateCatalogWithDestinationCatalogMissingObjectName() {
        // Create test data with missing destination object name
        val catalog = generateBasicApiCatalog()

        // Don't set destination object name - it will be null
        val configuredCatalog = catalogConverter.toConfiguredInternal(catalog)

        val destinationCatalog = DestinationCatalog(mutableListOf())

        val generationResult = CatalogGenerationResult(configuredCatalog, mapOf<StreamDescriptor, MutableMap<MapperConfig, MapperError>>())

        whenever(
          destinationCatalogGenerator.generateDestinationCatalog(
            configuredCatalog,
          ),
        ).thenReturn(generationResult)

        assertThrows(
          DestinationCatalogMissingObjectNameProblem::class.java,
        ) { connectionsHandler.validateCatalogWithDestinationCatalog(catalog, destinationCatalog) }
      }

      @Test
      fun testValidateCatalogWithDestinationCatalogInvalidOperation() {
        // Create test data with invalid operation (missing from destination catalog)
        val catalog = generateBasicApiCatalog()
        catalog
          .streams
          .first()
          .config
          .destinationObjectName("test_table")

        val configuredCatalog = catalogConverter.toConfiguredInternal(catalog)

        // No operations defined
        val destinationCatalog = DestinationCatalog(mutableListOf())

        val generationResult = CatalogGenerationResult(configuredCatalog, mapOf<StreamDescriptor, MutableMap<MapperConfig, MapperError>>())

        whenever(
          destinationCatalogGenerator.generateDestinationCatalog(
            configuredCatalog,
          ),
        ).thenReturn(generationResult)

        assertThrows(
          DestinationCatalogInvalidOperationProblem::class.java,
        ) { connectionsHandler.validateCatalogWithDestinationCatalog(catalog, destinationCatalog) }
      }

      @Test
      fun testValidateCatalogWithDestinationCatalogMissingRequiredField() {
        // Create test data with missing required field
        val catalog = generateBasicApiCatalog()
        catalog
          .streams
          .first()
          .config
          .destinationObjectName("test_table")

        val configuredCatalog = catalogConverter.toConfiguredInternal(catalog)

        val destinationSchema =
          jsonNode(
            mapOf<String?, Any?>(
              "type" to "object",
              "properties" to
                mapOf<String?, Any?>(
                  "field1" to mapOf<String?, String?>("type" to "string"),
                  "field2" to mapOf<String?, String?>("type" to "string"),
                ),
              "required" to mutableListOf<String?>("field2"), // field2 is required
            ),
          )

        val destinationCatalog =
          DestinationCatalog(
            listOf(
              DestinationOperation(
                "test_table",
                io.airbyte.config.DestinationSyncMode.APPEND,
                destinationSchema,
                null,
              ),
            ),
          )

        val generationResult = CatalogGenerationResult(configuredCatalog, mapOf<StreamDescriptor, MutableMap<MapperConfig, MapperError>>())

        whenever(
          destinationCatalogGenerator.generateDestinationCatalog(
            configuredCatalog,
          ),
        ).thenReturn(generationResult)

        assertThrows(
          DestinationCatalogMissingRequiredFieldProblem::class.java,
        ) { connectionsHandler.validateCatalogWithDestinationCatalog(catalog, destinationCatalog) }
      }

      @Test
      fun testValidateCatalogWithDestinationCatalogInvalidAdditionalField() {
        // Create test data with additional field not allowed
        val catalog = generateApiCatalogWithTwoFields()
        catalog
          .streams
          .first()
          .config
          .destinationObjectName("test_table")

        val configuredCatalog = catalogConverter.toConfiguredInternal(catalog)

        val destinationSchema =
          jsonNode(
            mapOf<String?, Any?>(
              "type" to "object",
              "properties" to
                mapOf<String?, Any?>(
                  FIELD_NAME to mapOf<String?, String?>("type" to "string"),
                ),
              "additionalProperties" to false, // Additional properties not allowed
            ),
          )

        val destinationCatalog =
          DestinationCatalog(
            listOf(
              DestinationOperation(
                "test_table",
                io.airbyte.config.DestinationSyncMode.APPEND,
                destinationSchema,
                null,
              ),
            ),
          )

        val generationResult = CatalogGenerationResult(configuredCatalog, mapOf<StreamDescriptor, MutableMap<MapperConfig, MapperError>>())

        whenever(
          destinationCatalogGenerator.generateDestinationCatalog(
            configuredCatalog,
          ),
        ).thenReturn(generationResult)

        assertThrows(
          DestinationCatalogInvalidAdditionalFieldProblem::class.java,
        ) { connectionsHandler.validateCatalogWithDestinationCatalog(catalog, destinationCatalog) }
      }

      @ParameterizedTest
      @MethodSource("io.airbyte.commons.server.handlers.ConnectionsHandlerTest#matchingKeysTestCases")
      fun testValidateCatalogWithDestinationCatalogMatchingKeys(testCase: MatchingKeyTestCase) {
        // Create test data
        val catalog = generateBasicApiCatalog()
        catalog
          .streams
          .first()
          .config
          .destinationObjectName("test_table")
          .primaryKey(testCase.primaryKey)

        val configuredCatalog = catalogConverter.toConfiguredInternal(catalog)

        val destinationSchema = generateBasicJsonSchema()
        val destinationCatalog =
          DestinationCatalog(
            listOf(
              DestinationOperation(
                "test_table",
                io.airbyte.config.DestinationSyncMode.APPEND,
                destinationSchema,
                testCase.matchingKeys,
              ),
            ),
          )

        val generationResult = CatalogGenerationResult(configuredCatalog, mapOf<StreamDescriptor, MutableMap<MapperConfig, MapperError>>())

        whenever(
          destinationCatalogGenerator.generateDestinationCatalog(
            configuredCatalog,
          ),
        ).thenReturn(generationResult)

        if (testCase.expectedException != null) {
          assertThrows(
            testCase.expectedException,
          ) { connectionsHandler.validateCatalogWithDestinationCatalog(catalog, destinationCatalog) }
        } else {
          assertDoesNotThrow {
            connectionsHandler.validateCatalogWithDestinationCatalog(catalog, destinationCatalog)
          }
        }
      }
    }
  }

  @Nested
  internal inner class ConnectionHistory {
    private fun generateMockAttemptWithStreamStats(
      attemptTime: Instant,
      streamsToRecordsSynced: List<Map<List<String?>?, Long?>?>,
    ): Attempt {
      val streamSyncStatsList =
        streamsToRecordsSynced
          .stream()
          .map { streamToRecordsSynced: Map<List<String?>?, Long?>? ->
            val streamKey: List<String?> =
              streamToRecordsSynced!!
                .keys
                .iterator()
                .next()
                ?.toList() ?: emptyList()
            val streamNamespace = streamKey[0]
            val streamName = streamKey[1]
            val recordsSynced: Long = streamToRecordsSynced[streamKey]!!
            StreamSyncStats()
              .withStreamName(streamName)
              .withStreamNamespace(streamNamespace)
              .withStats(SyncStats().withRecordsCommitted(recordsSynced))
          }.collect(Collectors.toList())

      val standardSyncSummary = StandardSyncSummary().withStreamStats(streamSyncStatsList)
      val standardSyncOutput = StandardSyncOutput().withStandardSyncSummary(standardSyncSummary)
      val jobOutput = JobOutput().withOutputType(JobOutput.OutputType.SYNC).withSync(standardSyncOutput)

      return Attempt(0, 0, null, null, jobOutput, AttemptStatus.FAILED, null, null, 0, 0, attemptTime.epochSecond)
    }

    private fun generateMockJob(
      connectionId: UUID,
      attempt: Attempt,
    ): Job =
      Job(
        0L,
        ConfigType.SYNC,
        connectionId.toString(),
        JobConfig(),
        listOf(attempt),
        JobStatus.RUNNING,
        1001L,
        1000L,
        1002L,
        true,
      )

    @Nested
    internal inner class GetConnectionDataHistory {
      @Test
      fun testGetConnectionDataHistory() {
        val connectionId = UUID.randomUUID()
        val numJobs = 10
        val apiReq = ConnectionDataHistoryRequestBody().numberOfJobs(numJobs).connectionId(connectionId)
        val jobOneId = 1L
        val jobTwoId = 2L

        val jobOne =
          Job(1, ConfigType.SYNC, connectionId.toString(), JobConfig(), mutableListOf(), JobStatus.SUCCEEDED, 0L, 0L, 0L, true)
        val jobTwo =
          Job(2, ConfigType.REFRESH, connectionId.toString(), JobConfig(), mutableListOf(), JobStatus.FAILED, 0L, 0L, 0L, true)

        whenever(
          jobPersistence.listJobs(
            Job.SYNC_REPLICATION_TYPES,
            setOf(JobStatus.SUCCEEDED, JobStatus.FAILED),
            apiReq.connectionId.toString(),
            apiReq.numberOfJobs,
          ),
        ).thenReturn(listOf(jobOne, jobTwo))

        val jobOneBytesCommitted = 12345L
        val jobOneBytesEmitted = 23456L
        val jobOneRecordsCommitted = 19L
        val jobOneRecordsEmmitted = 20L
        val jobOneCreatedAt = 1000L
        val jobOneUpdatedAt = 2000L
        val jobTwoCreatedAt = 3000L
        val jobTwoUpdatedAt = 4000L
        val jobTwoBytesCommitted = 98765L
        val jobTwoBytesEmmitted = 87654L
        val jobTwoRecordsCommitted = 50L
        val jobTwoRecordsEmittted = 60L
        val jobTwoRecordsRejected = 10L
        mockStatic(StatsAggregationHelper::class.java).use { mockStatsAggregationHelper ->
          mockStatsAggregationHelper
            .`when`<Any?> {
              getJobIdToJobWithAttemptsReadMap(
                anyOrNull(),
                anyOrNull(),
              )
            }.thenReturn(
              mapOf(
                jobOneId to
                  JobWithAttemptsRead().job(
                    JobRead().createdAt(jobOneCreatedAt).updatedAt(jobOneUpdatedAt).configType(JobConfigType.SYNC).aggregatedStats(
                      JobAggregatedStats()
                        .bytesCommitted(jobOneBytesCommitted)
                        .bytesEmitted(jobOneBytesEmitted)
                        .recordsCommitted(jobOneRecordsCommitted)
                        .recordsEmitted(jobOneRecordsEmmitted),
                    ),
                  ),
                jobTwoId to
                  JobWithAttemptsRead().job(
                    JobRead().createdAt(jobTwoCreatedAt).updatedAt(jobTwoUpdatedAt).configType(JobConfigType.SYNC).aggregatedStats(
                      JobAggregatedStats()
                        .bytesCommitted(jobTwoBytesCommitted)
                        .bytesEmitted(jobTwoBytesEmmitted)
                        .recordsCommitted(jobTwoRecordsCommitted)
                        .recordsRejected(jobTwoRecordsRejected)
                        .recordsEmitted(jobTwoRecordsEmittted),
                    ),
                  ),
              ),
            )
          val expected =
            listOf(
              JobSyncResultRead()
                .configType(JobConfigType.SYNC)
                .jobId(jobOneId)
                .bytesCommitted(jobOneBytesCommitted)
                .bytesEmitted(jobOneBytesEmitted)
                .recordsCommitted(jobOneRecordsCommitted)
                .recordsEmitted(jobOneRecordsEmmitted)
                .jobCreatedAt(jobOneCreatedAt)
                .jobUpdatedAt(jobOneUpdatedAt),
              JobSyncResultRead()
                .configType(JobConfigType.SYNC)
                .jobId(jobTwoId)
                .bytesCommitted(jobTwoBytesCommitted)
                .bytesEmitted(jobTwoBytesEmmitted)
                .recordsCommitted(jobTwoRecordsCommitted)
                .recordsEmitted(jobTwoRecordsEmittted)
                .recordsRejected(jobTwoRecordsRejected)
                .jobCreatedAt(jobTwoCreatedAt)
                .jobUpdatedAt(jobTwoUpdatedAt),
            )
          assertEquals(expected, connectionsHandler.getConnectionDataHistory(apiReq))
        }
      }
    }

    @Nested
    internal inner class GetConnectionStreamHistory {
      @Test
      @DisplayName("Handles empty history response")
      fun testStreamHistoryWithEmptyResponse() {
        val connectionId = UUID.randomUUID()
        val requestBody =
          ConnectionStreamHistoryRequestBody()
            .connectionId(connectionId)
            .timezone(timezoneLosAngeles)

        whenever(
          jobPersistence.listAttemptsForConnectionAfterTimestamp(
            eq(connectionId),
            eq(ConfigType.SYNC),
            anyOrNull(),
          ),
        ).thenReturn(mutableListOf())

        val actual =
          connectionsHandler.getConnectionStreamHistoryInternal(requestBody, Instant.ofEpochMilli(946684800000L))

        val expected = mutableListOf<ConnectionStreamHistoryReadItem?>()

        assertEquals(expected, actual)
      }

      @Test
      @DisplayName("Aggregates data correctly")
      fun testStreamHistoryAggregation() {
        val connectionId = UUID.randomUUID()
        val endTime = Instant.ofEpochMilli(946684800000L)
        val startTime = endTime.minus(30, ChronoUnit.DAYS)
        val attempt1Records = 100L
        val attempt2Records = 150L
        val attempt3Records = 200L
        val attempt4Records = 125L
        val streamName = "testStream"
        val streamNamespace = "testNamespace"
        val streamName2 = "testStream2"

        // First Attempt - Day 1
        val attempt1 =
          generateMockAttemptWithStreamStats(
            startTime.plus(1, ChronoUnit.DAYS),
            listOf(
              mapOf(
                listOf(streamNamespace, streamName) to attempt1Records,
              ),
            ),
          ) // 100 records
        val attemptWithJobInfo1 = fromJob(attempt1, generateMockJob(connectionId, attempt1))

        // Second Attempt - Same Day as First, same stream as first
        val attempt2 =
          generateMockAttemptWithStreamStats(
            startTime.plus(1, ChronoUnit.DAYS),
            listOf(
              mapOf(
                listOf(streamNamespace, streamName) to attempt2Records,
              ),
            ),
          ) // 100 records
        val attemptWithJobInfo2 = fromJob(attempt2, generateMockJob(connectionId, attempt2))

        // Third Attempt - Same Day, different stream
        val attempt3 =
          generateMockAttemptWithStreamStats(
            startTime.plus(1, ChronoUnit.DAYS),
            listOf(
              mapOf(
                listOf(streamNamespace, streamName2) to attempt3Records,
              ),
            ),
          ) // 100 records
        val attemptWithJobInfo3 = fromJob(attempt3, generateMockJob(connectionId, attempt3))

        // Fourth Attempt - Different day, first stream
        val attempt4 =
          generateMockAttemptWithStreamStats(
            startTime.plus(2, ChronoUnit.DAYS),
            listOf(
              mapOf(
                listOf(streamNamespace, streamName) to attempt4Records,
              ),
            ),
          ) // 100 records
        val attemptWithJobInfo4 = fromJob(attempt4, generateMockJob(connectionId, attempt4))

        val attempts = Arrays.asList(attemptWithJobInfo1, attemptWithJobInfo2, attemptWithJobInfo3, attemptWithJobInfo4)

        whenever(
          jobPersistence.listAttemptsForConnectionAfterTimestamp(
            eq(connectionId),
            eq(ConfigType.SYNC),
            anyOrNull(),
          ),
        ).thenReturn(attempts)

        val requestBody =
          ConnectionStreamHistoryRequestBody()
            .connectionId(connectionId)
            .timezone(timezoneLosAngeles)
        val actual: List<ConnectionStreamHistoryReadItem> =
          connectionsHandler.getConnectionStreamHistoryInternal(requestBody, endTime)

        val expected: MutableList<ConnectionStreamHistoryReadItem?> = mutableListOf()
        // expect the first entry to contain stream 1, day 1, 250 records... next item should be stream 2,
        // day 1, 200 records, and final entry should be stream 1, day 2, 125 records
        expected.add(
          ConnectionStreamHistoryReadItem()
            .timestamp(
              Math.toIntExact(
                startTime
                  .plus(1, ChronoUnit.DAYS)
                  .atZone(ZoneId.of(requestBody.timezone))
                  .toLocalDate()
                  .atStartOfDay(ZoneId.of(requestBody.timezone))
                  .toEpochSecond(),
              ),
            ).streamName(streamName)
            .streamNamespace(streamNamespace)
            .recordsCommitted(attempt1Records + attempt2Records),
        )
        expected.add(
          ConnectionStreamHistoryReadItem()
            .timestamp(
              Math.toIntExact(
                startTime
                  .plus(1, ChronoUnit.DAYS)
                  .atZone(ZoneId.of(requestBody.timezone))
                  .toLocalDate()
                  .atStartOfDay(ZoneId.of(requestBody.timezone))
                  .toEpochSecond(),
              ),
            ).streamName(streamName2)
            .streamNamespace(streamNamespace)
            .recordsCommitted(attempt3Records),
        )
        expected.add(
          ConnectionStreamHistoryReadItem()
            .timestamp(
              Math.toIntExact(
                startTime
                  .plus(2, ChronoUnit.DAYS)
                  .atZone(ZoneId.of(requestBody.timezone))
                  .toLocalDate()
                  .atStartOfDay(ZoneId.of(requestBody.timezone))
                  .toEpochSecond(),
              ),
            ).streamName(streamName)
            .streamNamespace(streamNamespace)
            .recordsCommitted(attempt4Records),
        )

        assertEquals(actual, expected)
      }
    }
  }

  @Nested
  internal inner class StreamConfigurationDiff {
    @Test
    fun testNoDiff() {
      val streamConfiguration1 =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration2 =
        getStreamConfiguration(
          listOf(cursor2),
          listOf(listOf(pk2)),
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null,
        )

      val catalog1 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )
      val catalog2 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )

      assertTrue(connectionsHandler.getConfigurationDiff(catalog1, catalog2).isEmpty())
    }

    @Test
    fun testNoDiffIfStreamAdded() {
      val streamConfiguration1 =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration2 =
        getStreamConfiguration(
          listOf(cursor2),
          listOf(listOf(pk2)),
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null,
        )

      val catalog1 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
            ),
          )
      val catalog2 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )

      assertTrue(connectionsHandler.getConfigurationDiff(catalog1, catalog2).isEmpty())
    }

    @Test
    fun testCursorOrderDoesMatter() {
      val streamConfiguration1 =
        getStreamConfiguration(
          listOf(cursor1, "anotherCursor"),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration1WithOtherCursorOrder =
        getStreamConfiguration(
          listOf("anotherCursor", cursor1),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration2 =
        getStreamConfiguration(
          listOf(cursor2),
          listOf(listOf(pk2)),
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null,
        )

      val catalog1 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )
      val catalog2 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1WithOtherCursorOrder),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )

      val changedSd: Set<io.airbyte.api.model.generated.StreamDescriptor> =
        connectionsHandler.getConfigurationDiff(catalog1, catalog2)
      assertFalse(changedSd.isEmpty())
      assertEquals(1, changedSd.size)
      assertEquals(
        setOf(
          io.airbyte.api.model.generated.StreamDescriptor().name(
            stream1,
          ),
        ),
        changedSd,
      )
    }

    @Test
    fun testPkOrderDoesntMatter() {
      val streamConfiguration1 =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1, pk3)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration1WithOtherPkOrder =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk3, pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration2 =
        getStreamConfiguration(
          listOf(cursor2),
          listOf(listOf(pk2), listOf(pk3)),
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null,
        )

      val streamConfiguration2WithOtherPkOrder =
        getStreamConfiguration(
          listOf(cursor2),
          listOf(listOf(pk3), listOf(pk2)),
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null,
        )

      val catalog1 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )
      val catalog2 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1WithOtherPkOrder),
              getStreamAndConfig(stream2, streamConfiguration2WithOtherPkOrder),
            ),
          )

      val changedSd: Set<io.airbyte.api.model.generated.StreamDescriptor> =
        connectionsHandler.getConfigurationDiff(catalog1, catalog2)
      assertFalse(changedSd.isEmpty())
      assertEquals(1, changedSd.size)
      assertEquals(
        setOf(
          io.airbyte.api.model.generated.StreamDescriptor().name(
            stream1,
          ),
        ),
        changedSd,
      )
    }

    @Test
    fun testNoDiffIfStreamRemove() {
      val streamConfiguration1 =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration2 =
        getStreamConfiguration(
          listOf(cursor2),
          listOf(listOf(pk2)),
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null,
        )

      val catalog1 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )
      val catalog2 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
            ),
          )

      assertTrue(connectionsHandler.getConfigurationDiff(catalog1, catalog2).isEmpty())
    }

    @Test
    fun testDiffDifferentCursor() {
      val streamConfiguration1 =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration1CursorDiff =
        getStreamConfiguration(
          listOf(cursor1, "anotherCursor"),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration2 =
        getStreamConfiguration(
          listOf(cursor2),
          listOf(listOf(pk2)),
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null,
        )

      val catalog1 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )
      val catalog2 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1CursorDiff),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )

      val changedSd: Set<io.airbyte.api.model.generated.StreamDescriptor?> =
        connectionsHandler.getConfigurationDiff(catalog1, catalog2)
      assertFalse(changedSd.isEmpty())
      assertEquals(1, changedSd.size)
      assertEquals(
        setOf(
          io.airbyte.api.model.generated.StreamDescriptor().name(
            stream1,
          ),
        ),
        changedSd,
      )
    }

    @Test
    fun testDiffIfDifferentPrimaryKey() {
      val streamConfiguration1 =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration1WithPkDiff =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1, pk3)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration2 =
        getStreamConfiguration(
          listOf(cursor2),
          listOf(listOf(pk2)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration2WithPkDiff =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1), listOf(pk3)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val catalog1 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )
      val catalog2 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1WithPkDiff),
              getStreamAndConfig(stream2, streamConfiguration2WithPkDiff),
            ),
          )

      val changedSd: Set<io.airbyte.api.model.generated.StreamDescriptor?> =
        connectionsHandler.getConfigurationDiff(catalog1, catalog2)
      assertFalse(changedSd.isEmpty())
      assertEquals(2, changedSd.size)
      org.assertj.core.api.Assertions
        .assertThat<io.airbyte.api.model.generated.StreamDescriptor?>(changedSd)
        .containsExactlyInAnyOrder(
          io.airbyte.api.model.generated
            .StreamDescriptor()
            .name(stream1),
          io.airbyte.api.model.generated.StreamDescriptor().name(
            stream2,
          ),
        )
    }

    @Test
    fun testDiffDifferentSyncMode() {
      val streamConfiguration1 =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration1CursorDiff =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration2 =
        getStreamConfiguration(
          listOf(cursor2),
          listOf(listOf(pk2)),
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null,
        )

      val catalog1 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )
      val catalog2 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1CursorDiff),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )

      val changedSd: Set<io.airbyte.api.model.generated.StreamDescriptor?> =
        connectionsHandler.getConfigurationDiff(catalog1, catalog2)
      assertFalse(changedSd.isEmpty())
      assertEquals(1, changedSd.size)
      assertEquals(
        setOf(
          io.airbyte.api.model.generated.StreamDescriptor().name(
            stream1,
          ),
        ),
        changedSd,
      )
    }

    @Test
    fun testGetCatalogDiffHandlesInvalidTypes() {
      val badCatalogJson =
        String(
          ConnectionsHandlerTest::class.java
            .getClassLoader()
            .getResourceAsStream("catalogs/catalog_with_invalid_type.json")
            .readAllBytes(),
          StandardCharsets.UTF_8,
        )

      val goodCatalogJson =
        String(
          ConnectionsHandlerTest::class.java
            .getClassLoader()
            .getResourceAsStream("catalogs/catalog_with_valid_type.json")
            .readAllBytes(),
          StandardCharsets.UTF_8,
        )

      val goodCatalogAlteredJson =
        String(
          ConnectionsHandlerTest::class.java
            .getClassLoader()
            .getResourceAsStream("catalogs/catalog_with_valid_type_and_update.json")
            .readAllBytes(),
          StandardCharsets.UTF_8,
        )

      val badConfigCatalogJson =
        String(
          ConnectionsHandlerTest::class.java
            .getClassLoader()
            .getResourceAsStream("catalogs/configured_catalog_with_invalid_type.json")
            .readAllBytes(),
          StandardCharsets.UTF_8,
        )

      val goodConfigCatalogJson =
        String(
          ConnectionsHandlerTest::class.java
            .getClassLoader()
            .getResourceAsStream("catalogs/configured_catalog_with_valid_type.json")
            .readAllBytes(),
          StandardCharsets.UTF_8,
        )

      // use Jsons.object to convert the json string to an AirbyteCatalog model
      val badCatalog: io.airbyte.protocol.models.v0.AirbyteCatalog =
        deserialize(
          badCatalogJson,
          io.airbyte.protocol.models.v0.AirbyteCatalog::class.java,
        )
      val goodCatalog: io.airbyte.protocol.models.v0.AirbyteCatalog =
        deserialize(
          goodCatalogJson,
          io.airbyte.protocol.models.v0.AirbyteCatalog::class.java,
        )
      val goodCatalogAltered: io.airbyte.protocol.models.v0.AirbyteCatalog =
        deserialize(
          goodCatalogAlteredJson,
          io.airbyte.protocol.models.v0.AirbyteCatalog::class.java,
        )
      val badConfiguredCatalog: ConfiguredAirbyteCatalog =
        deserialize(
          badConfigCatalogJson,
          ConfiguredAirbyteCatalog::class.java,
        )
      val goodConfiguredCatalog: ConfiguredAirbyteCatalog =
        deserialize(
          goodConfigCatalogJson,
          ConfiguredAirbyteCatalog::class.java,
        )

      // convert the AirbyteCatalog model to the AirbyteCatalog API model
      val convertedGoodCatalog = catalogConverter.toApi(goodCatalog, null)
      val convertedGoodCatalogAltered = catalogConverter.toApi(goodCatalogAltered, null)
      val convertedBadCatalog = catalogConverter.toApi(badCatalog, null)

      // No issue for valid catalogs
      val gggDiff = connectionsHandler.getDiff(convertedGoodCatalog, convertedGoodCatalog, goodConfiguredCatalog, connectionId)
      assertEquals(gggDiff.transforms.size, 0)
      val ggagDiff = connectionsHandler.getDiff(convertedGoodCatalog, convertedGoodCatalogAltered, goodConfiguredCatalog, connectionId)
      assertEquals(ggagDiff.transforms.size, 1)

      // No issue for good catalog and a bad configured catalog
      val ggbDiff = connectionsHandler.getDiff(convertedGoodCatalog, convertedGoodCatalog, badConfiguredCatalog, connectionId)
      assertEquals(ggbDiff.transforms.size, 0)
      val ggabDiff = connectionsHandler.getDiff(convertedGoodCatalog, convertedGoodCatalogAltered, badConfiguredCatalog, connectionId)
      assertEquals(ggabDiff.transforms.size, 1)

      // assert no issue when migrating two or from a catalog with a skippable (slightly invalid) type
      val bggDiff = connectionsHandler.getDiff(convertedBadCatalog, convertedGoodCatalog, goodConfiguredCatalog, connectionId)
      assertEquals(bggDiff.transforms.size, 1)

      val gbgDiff = connectionsHandler.getDiff(convertedGoodCatalog, convertedBadCatalog, goodConfiguredCatalog, connectionId)
      assertEquals(gbgDiff.transforms.size, 1)
    }

    @Test
    fun testDiffDifferentDestinationSyncMode() {
      val streamConfiguration1 =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND_DEDUP,
          null,
        )

      val streamConfiguration1CursorDiff =
        getStreamConfiguration(
          listOf(cursor1),
          listOf(listOf(pk1)),
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
          DestinationSyncMode.APPEND,
          null,
        )

      val streamConfiguration2 =
        getStreamConfiguration(
          listOf(cursor2),
          listOf(listOf(pk2)),
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          DestinationSyncMode.OVERWRITE,
          null,
        )

      val catalog1 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )
      val catalog2 =
        AirbyteCatalog()
          .streams(
            listOf<@Valid AirbyteStreamAndConfiguration?>(
              getStreamAndConfig(stream1, streamConfiguration1CursorDiff),
              getStreamAndConfig(stream2, streamConfiguration2),
            ),
          )

      val changedSd: Set<io.airbyte.api.model.generated.StreamDescriptor> =
        connectionsHandler.getConfigurationDiff(catalog1, catalog2)
      assertFalse(changedSd.isEmpty())
      assertEquals(1, changedSd.size)
      assertEquals(
        setOf(
          io.airbyte.api.model.generated.StreamDescriptor().name(
            stream1,
          ),
        ),
        changedSd,
      )
    }

    @Test
    fun testConnectionStatus() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val failureSummary = AttemptFailureSummary()
      failureSummary.setFailures(listOf(FailureReason().withFailureOrigin(FailureReason.FailureOrigin.DESTINATION)))
      val failedAttempt = Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, failureSummary, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            0L,
            ConfigType.SYNC,
            connectionId.toString(),
            JobConfig(),
            mutableListOf(),
            JobStatus.RUNNING,
            1001L,
            1000L,
            1002L,
            true,
          ),
          Job(
            1L,
            ConfigType.SYNC,
            connectionId.toString(),
            JobConfig(),
            listOf(failedAttempt),
            JobStatus.FAILED,
            901L,
            900L,
            902L,
            true,
          ),
          Job(2L, ConfigType.SYNC, connectionId.toString(), JobConfig(), mutableListOf(), JobStatus.SUCCEEDED, 801L, 800L, 802L, true),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      assertEquals(1, status.size)

      val connectionStatus = status[0]
      assertEquals(connectionId, connectionStatus.connectionId)
      assertEquals(802L, connectionStatus.lastSuccessfulSync)
      assertEquals(0L, connectionStatus.activeJob.id)
    }

    @Test
    fun testConnectionStatus_syncing() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val attempt = Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            0L,
            ConfigType.SYNC,
            connectionId.toString(),
            JobConfig(),
            mutableListOf(),
            JobStatus.RUNNING,
            1001L,
            1000L,
            1002L,
            true,
          ),
          Job(
            1L,
            ConfigType.SYNC,
            connectionId.toString(),
            JobConfig(),
            listOf(attempt),
            JobStatus.SUCCEEDED,
            801L,
            800L,
            802L,
            true,
          ),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.RUNNING.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @Test
    fun testConnectionStatus_failed_breakingSchemaChange() {
      val standardSyncWithBreakingSchemaChange = clone(standardSync).withBreakingChange(true)
      whenever(connectionService.getStandardSync(standardSync.connectionId))
        .thenReturn(standardSyncWithBreakingSchemaChange)

      val connectionId = standardSync.connectionId
      val attempt = Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            1L,
            ConfigType.SYNC,
            connectionId.toString(),
            JobConfig(),
            listOf(attempt),
            JobStatus.SUCCEEDED,
            801L,
            800L,
            802L,
            true,
          ),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.FAILED.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @Test
    fun testConnectionStatus_failed_hasConfigError() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val failureSummary = AttemptFailureSummary()
      failureSummary.setFailures(
        listOf(
          FailureReason()
            .withFailureType(FailureReason.FailureType.CONFIG_ERROR)
            .withFailureOrigin(FailureReason.FailureOrigin.DESTINATION),
        ),
      )
      val attempt = Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, failureSummary, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            1L,
            ConfigType.SYNC,
            connectionId.toString(),
            JobConfig(),
            listOf(attempt),
            JobStatus.FAILED,
            801L,
            800L,
            802L,
            true,
          ),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.FAILED.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @ParameterizedTest
    @EnumSource(StandardSync.Status::class, names = ["INACTIVE", "DEPRECATED", "LOCKED"])
    fun testConnectionStatus_paused_if_not_active(status: StandardSync.Status) {
      val standardSyncPaused = clone(standardSync).withStatus(status)
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSyncPaused)

      val connectionId = standardSync.connectionId
      val attempt = Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            1L,
            ConfigType.SYNC,
            connectionId.toString(),
            JobConfig(),
            listOf(attempt),
            JobStatus.SUCCEEDED,
            801L,
            800L,
            802L,
            true,
          ),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.PAUSED.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @Test
    fun testConnectionStatus_pending_nosyncs() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val jobs = listOf<Job>()
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.PENDING.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @Test
    fun testConnectionStatus_pending_afterSuccessfulReset() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val attempt = Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            0L,
            ConfigType.RESET_CONNECTION,
            connectionId.toString(),
            JobConfig(),
            listOf(attempt),
            JobStatus.SUCCEEDED,
            1001L,
            1000L,
            1002L,
            true,
          ),
          Job(1L, ConfigType.SYNC, connectionId.toString(), JobConfig(), mutableListOf(), JobStatus.SUCCEEDED, 801L, 800L, 802L, true),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.PENDING.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @Test
    fun testConnectionStatus_pending_afterFailedReset() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val attempt = Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, null, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            0L,
            ConfigType.RESET_CONNECTION,
            connectionId.toString(),
            JobConfig(),
            listOf(attempt),
            JobStatus.FAILED,
            1001L,
            1000L,
            1002L,
            true,
          ),
          Job(1L, ConfigType.SYNC, connectionId.toString(), JobConfig(), mutableListOf(), JobStatus.SUCCEEDED, 801L, 800L, 802L, true),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.PENDING.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @Test
    fun testConnectionStatus_pending_afterSuccessfulClear() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val attempt = Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            0L,
            ConfigType.CLEAR,
            connectionId.toString(),
            JobConfig(),
            listOf(attempt),
            JobStatus.SUCCEEDED,
            1001L,
            1000L,
            1002L,
            true,
          ),
          Job(1L, ConfigType.SYNC, connectionId.toString(), JobConfig(), mutableListOf(), JobStatus.SUCCEEDED, 801L, 800L, 802L, true),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.PENDING.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @Test
    fun testConnectionStatus_pending_afterFailedClear() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val attempt = Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, null, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            0L,
            ConfigType.CLEAR,
            connectionId.toString(),
            JobConfig(),
            listOf(attempt),
            JobStatus.FAILED,
            1001L,
            1000L,
            1002L,
            true,
          ),
          Job(1L, ConfigType.SYNC, connectionId.toString(), JobConfig(), mutableListOf(), JobStatus.SUCCEEDED, 801L, 800L, 802L, true),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.PENDING.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @Test
    fun testConnectionStatus_incomplete_afterCancelledReset() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val resetAttempt = Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, null, 0, 0, 0L)
      val successAttempt = Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            0L,
            ConfigType.RESET_CONNECTION,
            connectionId.toString(),
            JobConfig(),
            listOf(resetAttempt),
            JobStatus.CANCELLED,
            1001L,
            1000L,
            1002L,
            true,
          ),
          Job(
            1L,
            ConfigType.SYNC,
            connectionId.toString(),
            JobConfig(),
            listOf(successAttempt),
            JobStatus.SUCCEEDED,
            801L,
            800L,
            802L,
            true,
          ),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.INCOMPLETE.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @Test
    fun testConnectionStatus_incomplete_failed() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val attempt = Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, null, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            0L,
            ConfigType.SYNC,
            connectionId.toString(),
            JobConfig(),
            listOf(attempt),
            JobStatus.FAILED,
            1001L,
            1000L,
            1002L,
            true,
          ),
          Job(1L, ConfigType.SYNC, connectionId.toString(), JobConfig(), mutableListOf(), JobStatus.SUCCEEDED, 801L, 800L, 802L, true),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.INCOMPLETE.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @Test
    fun testConnectionStatus_incomplete_cancelled() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val failedAttempt = Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, null, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            0L,
            ConfigType.SYNC,
            connectionId.toString(),
            JobConfig(),
            listOf(failedAttempt),
            JobStatus.CANCELLED,
            1001L,
            1000L,
            1002L,
            true,
          ),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.INCOMPLETE.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    @Test
    fun testConnectionStatus_synced() {
      whenever(connectionService.getStandardSync(standardSync.connectionId)).thenReturn(standardSync)

      val connectionId = standardSync.connectionId
      val attempt = Attempt(0, 0, null, null, null, AttemptStatus.SUCCEEDED, null, null, 0, 0, 0L)
      val jobs =
        listOf(
          Job(
            0L,
            ConfigType.SYNC,
            connectionId.toString(),
            JobConfig(),
            listOf(attempt),
            JobStatus.SUCCEEDED,
            1001L,
            1000L,
            1002L,
            true,
          ),
        )
      whenever(
        jobPersistence.listJobsLight(
          Job.Companion.REPLICATION_TYPES,
          connectionId.toString(),
          10,
        ),
      ).thenReturn(jobs)
      val req = ConnectionStatusesRequestBody().connectionIds(listOf(connectionId))
      val status: List<ConnectionStatusRead> = connectionsHandler.getConnectionStatuses(req)
      val connectionStatus = status[0]
      assertEquals(
        ConnectionSyncStatus.SYNCED.convertTo<ConnectionSyncStatus>(),
        connectionStatus.connectionSyncStatus,
      )
    }

    private fun getStreamAndConfig(
      name: String?,
      config: AirbyteStreamConfiguration?,
    ): AirbyteStreamAndConfiguration? =
      AirbyteStreamAndConfiguration()
        .config(config)
        .stream(AirbyteStream().name(name))

    private fun getStreamConfiguration(
      cursors: List<String?>?,
      primaryKeys: List<List<String?>?>?,
      syncMode: io.airbyte.api.model.generated.SyncMode?,
      destinationSyncMode: DestinationSyncMode?,
      hashedFields: MutableList<SelectedFieldInfo?>?,
    ): AirbyteStreamConfiguration? =
      AirbyteStreamConfiguration()
        .cursorField(cursors)
        .primaryKey(primaryKeys)
        .syncMode(syncMode)
        .destinationSyncMode(destinationSyncMode)
        .hashedFields(hashedFields)
  }

  /**
   * Tests for the applySchemaChanges endpoint. Note that most of the core auto-propagation logic is
   * tested directly on AutoPropagateSchemaChangeHelper.getUpdatedSchema().
   */
  @Nested
  internal inner class ApplySchemaChanges {
    @BeforeEach
    fun setup() {
      airbyteCatalog
        .streams
        .get(0)
        .withSupportedSyncModes(listOf(io.airbyte.protocol.models.v0.SyncMode.FULL_REFRESH))
      standardSync =
        StandardSync()
          .withConnectionId(connectionId)
          .withSourceId(sourceId)
          .withDestinationId(destinationId)
          .withCatalog(configuredAirbyteCatalog)
          .withStatus(StandardSync.Status.ACTIVE)
          .withManual(true)
          .withNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.PROPAGATE_FULLY)

      val destinationDefinition =
        StandardDestinationDefinition()
          .withDestinationDefinitionId(destinationDefinitionId)
      val sourceDefinition =
        StandardSourceDefinition()
          .withSourceDefinitionId(UUID.randomUUID())

      val sourceVersion = mock(ActorDefinitionVersion::class.java)
      val destinationVersion = mock(ActorDefinitionVersion::class.java)
      whenever(sourceVersion.supportsFileTransfer).thenReturn(false)
      whenever(destinationVersion.supportsFileTransfer).thenReturn(false)
      whenever(
        actorDefinitionVersionHelper.getSourceVersion(
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(sourceVersion)
      whenever(
        actorDefinitionVersionHelper.getDestinationVersion(
          anyOrNull(),
          anyOrNull(),
          anyOrNull(),
        ),
      ).thenReturn(destinationVersion)

      whenever(catalogService.getActorCatalogById(sourceCatalogId)).thenReturn(actorCatalog)
      whenever(connectionService.getStandardSync(connectionId)).thenReturn(standardSync)
      whenever(sourceService.getSourceConnection(sourceId)).thenReturn(source)
      whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)).thenReturn(workspace)
      whenever(destinationService.getDestinationDefinitionFromConnection(connectionId))
        .thenReturn(destinationDefinition)
      whenever(sourceService.getSourceDefinitionFromConnection(connectionId)).thenReturn(sourceDefinition)
      whenever(
        connectorDefinitionSpecificationHandler.getDestinationSpecification(
          DestinationDefinitionIdWithWorkspaceId()
            .workspaceId(workspaceId)
            .destinationDefinitionId(destinationDefinitionId),
        ),
      ).thenReturn(
        DestinationDefinitionSpecificationRead().supportedDestinationSyncModes(
          listOf(
            DestinationSyncMode.OVERWRITE,
          ),
        ),
      )
      whenever(workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(sourceId)).thenReturn(workspaceId)
      whenever(workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(destinationId)).thenReturn(workspaceId)
      whenever(workspaceHelper.getWorkspaceForConnectionId(connectionId)).thenReturn(workspaceId)
    }

    @Test
    fun testAutoPropagateSchemaChange() {
      // Somehow standardSync is being mutated in the test (the catalog is changed) and verifying that the
      // notification function is called correctly requires the original object.
      val originalSync = clone(standardSync!!)
      val catalogWithDiff =
        catalogConverter.toApi(clone(airbyteCatalog), sourceVersion)
      catalogWithDiff.addStreamsItem(
        AirbyteStreamAndConfiguration()
          .stream(
            AirbyteStream()
              .name(aDifferentStream)
              .namespace(aDifferentNamespace)
              .sourceDefinedCursor(false)
              .jsonSchema(emptyObject())
              .supportedSyncModes(
                listOf<io.airbyte.api.model.generated.SyncMode?>(io.airbyte.api.model.generated.SyncMode.FULL_REFRESH),
              ),
          ).config(
            AirbyteStreamConfiguration()
              .syncMode(io.airbyte.api.model.generated.SyncMode.FULL_REFRESH)
              .destinationSyncMode(DestinationSyncMode.OVERWRITE)
              .selected(true),
          ),
      )

      val request =
        ConnectionAutoPropagateSchemaChange()
          .connectionId(connectionId)
          .workspaceId(workspaceId)
          .catalogId(sourceCatalogId)
          .catalog(catalogWithDiff)

      val actualResult = connectionsHandler.applySchemaChange(request)

      val expectedDiff =
        CatalogDiff().addTransformsItem(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
            .streamDescriptor(
              io.airbyte.api.model.generated
                .StreamDescriptor()
                .namespace(aDifferentNamespace)
                .name(aDifferentStream),
            ),
        )
      assertEquals(expectedDiff, actualResult.propagatedDiff)
      val expectedCatalogTmp = clone(configuredAirbyteCatalog)
      expectedCatalogTmp.streams.forEach(Consumer { s: ConfiguredAirbyteStream? -> s!!.stream.withSourceDefinedCursor(false) })

      val streamsTmp =
        expectedCatalogTmp.streams +
          listOf(
            ConfiguredAirbyteStream
              .Builder()
              .stream(
                io.airbyte.config
                  .AirbyteStream(aDifferentStream, emptyObject(), listOf(SyncMode.FULL_REFRESH))
                  .withNamespace(aDifferentNamespace)
                  .withSourceDefinedCursor(false)
                  .withDefaultCursorField(mutableListOf()),
              ).syncMode(SyncMode.FULL_REFRESH)
              .destinationSyncMode(io.airbyte.config.DestinationSyncMode.OVERWRITE)
              .cursorField(mutableListOf())
              .fields(mutableListOf())
              .build(),
          )

      val expectedCatalog = ConfiguredAirbyteCatalog(streamsTmp)
      val standardSyncArgumentCaptor = argumentCaptor<StandardSync>()
      verify(connectionService).writeStandardSync(standardSyncArgumentCaptor.capture())
      val actualStandardSync = standardSyncArgumentCaptor.firstValue
      assertEquals(clone(standardSync!!).withCatalog(expectedCatalog), actualStandardSync)
      // the notification function is being called with copy of the originalSync that does not contain the
      // updated catalog
      // This is ok as we only pass that object to get connectionId and connectionName
      verify(notificationHelper).notifySchemaPropagated(
        notificationSettings,
        expectedDiff,
        workspace,
        apiPojoConverters.internalToConnectionRead(originalSync),
        source,
        email,
      )
    }

    @Test
    fun testAutoPropagateColumnsOnly() {
      // See test above for why this part is necessary.
      val originalSync = clone(standardSync!!)
      val newField = Field.of(aDifferentColumn, JsonSchemaType.STRING)
      val catalogWithDiff =
        catalogConverter.toApi(
          CatalogHelpers.createAirbyteCatalog(
            shoes,
            Field.of(sku, JsonSchemaType.STRING),
            newField,
          ),
          sourceVersion,
        )

      val request =
        ConnectionAutoPropagateSchemaChange()
          .connectionId(connectionId)
          .workspaceId(workspaceId)
          .catalogId(sourceCatalogId)
          .catalog(catalogWithDiff)

      val actualResult = connectionsHandler.applySchemaChange(request)

      val expectedDiff =
        CatalogDiff().addTransformsItem(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(
              io.airbyte.api.model.generated
                .StreamDescriptor()
                .namespace(null)
                .name(shoes),
            ).updateStream(
              StreamTransformUpdateStream().addFieldTransformsItem(
                FieldTransform()
                  .addField(FieldAdd().schema(deserialize("{\"type\": \"string\"}")))
                  .fieldName(listOf(newField.name))
                  .breaking(false)
                  .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD),
              ),
            ),
        )
      assertEquals(expectedDiff, actualResult.propagatedDiff)
      verify(notificationHelper).notifySchemaPropagated(
        notificationSettings,
        expectedDiff,
        workspace,
        apiPojoConverters.internalToConnectionRead(originalSync),
        source,
        email,
      )
    }

    @Test
    fun testSendingNotificationToManuallyApplySchemaChange() {
      // Override the non-breaking changes preference to ignore so that the changes are not
      // auto-propagated, but needs to be manually applied.
      standardSync!!.setNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.IGNORE)
      whenever(connectionService.getStandardSync(connectionId)).thenReturn(standardSync)
      val originalSync = clone(standardSync!!)
      val newField = Field.of(aDifferentColumn, JsonSchemaType.STRING)
      val catalogWithDiff =
        catalogConverter.toApi(
          CatalogHelpers.createAirbyteCatalog(
            shoes,
            Field.of(sku, JsonSchemaType.STRING),
            newField,
          ),
          sourceVersion,
        )

      val request =
        ConnectionAutoPropagateSchemaChange()
          .connectionId(connectionId)
          .workspaceId(workspaceId)
          .catalogId(sourceCatalogId)
          .catalog(catalogWithDiff)

      connectionsHandler.applySchemaChange(request)

      val expectedDiff =
        CatalogDiff().addTransformsItem(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(
              io.airbyte.api.model.generated
                .StreamDescriptor()
                .namespace(null)
                .name(shoes),
            ).updateStream(
              StreamTransformUpdateStream().addFieldTransformsItem(
                FieldTransform()
                  .addField(FieldAdd().schema(deserialize("{\"type\": \"string\"}")))
                  .fieldName(listOf(newField.name))
                  .breaking(false)
                  .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD),
              ),
            ),
        )

      verify(notificationHelper).notifySchemaDiffToApply(
        notificationSettings,
        expectedDiff,
        workspace,
        apiPojoConverters.internalToConnectionRead(originalSync),
        source,
        email,
        false,
      )
    }

    @Test
    fun testSendingNotificationToManuallyApplySchemaChangeWithPropagationDisabled() {
      // Override the non-breaking changes preference to DISABLE so that the changes are not
      // auto-propagated, but needs to be manually applied.
      standardSync!!.setNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.DISABLE)
      whenever(connectionService.getStandardSync(connectionId)).thenReturn(standardSync)
      val originalSync = clone(standardSync!!)
      val newField = Field.of(aDifferentColumn, JsonSchemaType.STRING)
      val catalogWithDiff =
        catalogConverter.toApi(
          CatalogHelpers.createAirbyteCatalog(
            shoes,
            Field.of(sku, JsonSchemaType.STRING),
            newField,
          ),
          sourceVersion,
        )

      val request =
        ConnectionAutoPropagateSchemaChange()
          .connectionId(connectionId)
          .workspaceId(workspaceId)
          .catalogId(sourceCatalogId)
          .catalog(catalogWithDiff)

      connectionsHandler.applySchemaChange(request)

      val expectedDiff =
        CatalogDiff().addTransformsItem(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(
              io.airbyte.api.model.generated
                .StreamDescriptor()
                .namespace(null)
                .name(shoes),
            ).updateStream(
              StreamTransformUpdateStream().addFieldTransformsItem(
                FieldTransform()
                  .addField(FieldAdd().schema(deserialize("{\"type\": \"string\"}")))
                  .fieldName(listOf(newField.name))
                  .breaking(false)
                  .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD),
              ),
            ),
        )

      verify(notificationHelper).notifySchemaDiffToApply(
        notificationSettings,
        expectedDiff,
        workspace,
        apiPojoConverters.internalToConnectionRead(originalSync),
        source,
        email,
        true,
      )
    }

    @Test
    fun diffCatalogGeneratesADiffAndUpdatesTheConnection() {
      val newField = Field.of(aDifferentColumn, JsonSchemaType.STRING)
      val catalogWithDiff =
        CatalogHelpers.createAirbyteCatalog(shoes, Field.of(sku, JsonSchemaType.STRING), newField)
      val discoveredCatalog =
        ActorCatalog()
          .withCatalog(jsonNode<io.airbyte.protocol.models.v0.AirbyteCatalog?>(catalogWithDiff))
          .withCatalogHash("")
          .withId(UUID.randomUUID())
      whenever(catalogService.getActorCatalogById(discoveredCatalogId)).thenReturn(discoveredCatalog)

      val expectedDiff =
        CatalogDiff().addTransformsItem(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(
              io.airbyte.api.model.generated
                .StreamDescriptor()
                .namespace(null)
                .name(shoes),
            ).updateStream(
              StreamTransformUpdateStream().addFieldTransformsItem(
                FieldTransform()
                  .addField(FieldAdd().schema(deserialize("{\"type\": \"string\"}")))
                  .fieldName(listOf(newField.name))
                  .breaking(false)
                  .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD),
              ),
            ),
        )

      val result = connectionsHandler.diffCatalogAndConditionallyDisable(connectionId, discoveredCatalogId)

      assertEquals(expectedDiff, result.catalogDiff)
      assertEquals(false, result.breakingChange)

      val syncCaptor = argumentCaptor<StandardSync>()
      verify(connectionService).writeStandardSync(syncCaptor.capture())
      val savedSync = syncCaptor.firstValue
      assertNotEquals(StandardSync.Status.INACTIVE, savedSync.status)
    }

    @Test
    fun diffCatalogADisablesForBreakingChange() {
      val helper = mock(ApplySchemaChangeHelper::class.java)
      val field = ConnectionsHandler::class.java.getDeclaredField("applySchemaChangeHelper")
      field.setAccessible(true)
      field.set(connectionsHandler, helper)

      whenever(helper.containsBreakingChange(anyOrNull())).thenReturn(true)

      val result = connectionsHandler.diffCatalogAndConditionallyDisable(connectionId, sourceCatalogId)
      assertEquals(true, result.breakingChange)

      val syncCaptor = argumentCaptor<StandardSync>()
      verify(connectionService).writeStandardSync(syncCaptor.capture())
      val savedSync = syncCaptor.firstValue
      assertEquals(StandardSync.Status.INACTIVE, savedSync.status)
    }

    @Test
    fun diffCatalogDisablesForNonBreakingChangeIfConfiguredSo() {
      // configure the sync to be disabled on non-breaking change
      standardSync = standardSync!!.withNonBreakingChangesPreference(StandardSync.NonBreakingChangesPreference.DISABLE)
      whenever(connectionService.getStandardSync(connectionId)).thenReturn(standardSync)

      val newField = Field.of(aDifferentColumn, JsonSchemaType.STRING)
      val catalogWithDiff =
        CatalogHelpers.createAirbyteCatalog(shoes, Field.of(sku, JsonSchemaType.STRING), newField)
      val discoveredCatalog =
        ActorCatalog()
          .withCatalog(jsonNode<io.airbyte.protocol.models.v0.AirbyteCatalog?>(catalogWithDiff))
          .withCatalogHash("")
          .withId(UUID.randomUUID())
      whenever(catalogService.getActorCatalogById(discoveredCatalogId)).thenReturn(discoveredCatalog)

      val result = connectionsHandler.diffCatalogAndConditionallyDisable(connectionId, discoveredCatalogId)

      assertEquals(false, result.breakingChange)

      val syncCaptor = argumentCaptor<StandardSync>()
      verify(connectionService).writeStandardSync(syncCaptor.capture())
      val savedSync = syncCaptor.firstValue
      assertEquals(StandardSync.Status.INACTIVE, savedSync.status)
    }

    @Test
    fun postprocessDiscoveredComposesDiffingAndSchemaPropagation() {
      val catalog = catalogConverter.toApi(clone(airbyteCatalog), sourceVersion)
      val diffResult = SourceDiscoverSchemaRead().catalog(catalog)
      val transform =
        StreamTransform()
          .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
          .streamDescriptor(
            io.airbyte.api.model.generated
              .StreamDescriptor()
              .namespace(aDifferentNamespace)
              .name(aDifferentStream),
          )
      val propagatedDiff = CatalogDiff().transforms(listOf<@Valid StreamTransform?>(transform))
      val autoPropResult = ConnectionAutoPropagateResult().propagatedDiff(propagatedDiff)

      val spiedConnectionsHandler = spy(connectionsHandler)
      doReturn(diffResult)
        .`when`(spiedConnectionsHandler)
        .diffCatalogAndConditionallyDisable(connectionId, discoveredCatalogId)
      doReturn(autoPropResult)
        .`when`(spiedConnectionsHandler)
        .applySchemaChange(connectionId, workspaceId, discoveredCatalogId, catalog, true)

      val result = spiedConnectionsHandler.postprocessDiscoveredCatalog(connectionId, discoveredCatalogId)

      assertEquals(propagatedDiff, result.appliedDiff)
    }

    @Test
    fun postprocessDiscoveredComposesDiffingAndSchemaPropagationUsesMostRecentCatalog() {
      val catalog = catalogConverter.toApi(clone(airbyteCatalog), sourceVersion)
      val diffResult = SourceDiscoverSchemaRead().catalog(catalog)
      val transform =
        StreamTransform()
          .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
          .streamDescriptor(
            io.airbyte.api.model.generated
              .StreamDescriptor()
              .namespace(aDifferentNamespace)
              .name(aDifferentStream),
          )
      val propagatedDiff = CatalogDiff().transforms(listOf<@Valid StreamTransform?>(transform))
      val autoPropResult = ConnectionAutoPropagateResult().propagatedDiff(propagatedDiff)

      val mostRecentCatalogId = UUID.randomUUID()
      val mostRecentCatalog = ActorCatalogWithUpdatedAt().withId(mostRecentCatalogId)
      doReturn(Optional.of(mostRecentCatalog))
        .`when`(catalogService)
        .getMostRecentSourceActorCatalog(
          sourceId,
        )

      val spiedConnectionsHandler = spy(connectionsHandler)

      doReturn(diffResult)
        .`when`(spiedConnectionsHandler)
        .diffCatalogAndConditionallyDisable(connectionId, mostRecentCatalogId)

      doReturn(autoPropResult)
        .`when`(spiedConnectionsHandler)
        .applySchemaChange(connectionId, workspaceId, mostRecentCatalogId, catalog, true)

      val result = spiedConnectionsHandler.postprocessDiscoveredCatalog(connectionId, discoveredCatalogId)

      assertEquals(propagatedDiff, result.appliedDiff)
    }

    @Test
    fun getConnectionContextReturnsConnectionContext() {
      val connectionId = UUID.randomUUID()
      val context =
        ConnectionContext()
          .withConnectionId(connectionId)
          .withSourceId(UUID.randomUUID())
          .withDestinationId(UUID.randomUUID())
          .withSourceDefinitionId(UUID.randomUUID())
          .withDestinationDefinitionId(UUID.randomUUID())
          .withWorkspaceId(UUID.randomUUID())
          .withOrganizationId(UUID.randomUUID())
      doReturn(context).`when`(contextBuilder).fromConnectionId(connectionId)
      val result = connectionsHandler.getConnectionContext(connectionId)
      val expected = context.toServerApi()
      assertEquals(expected, result)
    }

    private val sourceProtocolVersion = "0.4.5"
    private val shoes = "shoes"
    private val sku = "sku"
    private val aDifferentStream = "a-different-stream"
    private val sourceVersion: ActorDefinitionVersion? =
      ActorDefinitionVersion()
        .withProtocolVersion(sourceProtocolVersion)

    private val sourceCatalogId: UUID = UUID.randomUUID()
    private val connectionId: UUID = UUID.randomUUID()
    private val sourceId: UUID = UUID.randomUUID()
    private val destinationDefinitionId: UUID = UUID.randomUUID()
    private val workspaceId: UUID = UUID.randomUUID()
    private val destinationId: UUID = UUID.randomUUID()
    private val discoveredCatalogId: UUID = UUID.randomUUID()
    private val catalogHelpers =
      io.airbyte.config.helpers
        .CatalogHelpers(FieldGenerator())
    private val airbyteCatalog: io.airbyte.protocol.models.v0.AirbyteCatalog =
      CatalogHelpers.createAirbyteCatalog(shoes, Field.of(sku, JsonSchemaType.STRING))
    private val configuredAirbyteCatalog = catalogHelpers.createConfiguredAirbyteCatalog(shoes, null, Field.of(sku, JsonSchemaType.STRING))
    private val aDifferentNamespace = "a-different-namespace"
    private val aDifferentColumn = "a-different-column"
    private var standardSync: StandardSync? = null
    private val notificationSettings = NotificationSettings()
    private val email = "WorkspaceEmail@Company.com"
    private val workspace: StandardWorkspace =
      StandardWorkspace()
        .withWorkspaceId(workspaceId)
        .withEmail(email)
        .withNotificationSettings(notificationSettings)
    private val actorCatalog: ActorCatalog? =
      ActorCatalog()
        .withCatalog(jsonNode<io.airbyte.protocol.models.v0.AirbyteCatalog?>(airbyteCatalog))
        .withCatalogHash("")
        .withId(UUID.randomUUID())
  }

  @Nested
  internal inner class ConnectionLastJobPerStream {
    @Test
    fun testGetConnectionLastJobPerStream() {
      val connectionId = UUID.randomUUID()
      val jobId = 1L
      val stream1Name = "testStream1"
      val stream1Namespace = "testNamespace1"
      val stream2Name = "testStream2"
      val stream1Descriptor =
        StreamDescriptor()
          .withName(stream1Name)
          .withNamespace(stream1Namespace)
      val stream2Descriptor =
        StreamDescriptor()
          .withName(stream2Name)
          .withNamespace(null)
      val createdAt = Instant.now()
      val startedAt = createdAt.plusSeconds(10)
      val updatedAt = startedAt.plusSeconds(10)
      val bytesCommitted1 = 12345L
      val bytesEmitted1 = 23456L
      val recordsCommitted1 = 100L
      val recordsEmitted1 = 200L
      val bytesCommitted2 = 912345L
      val bytesEmitted2 = 923456L
      val recordsCommitted2 = 9100L
      val recordsEmitted2 = 9200L
      val configType = ConfigType.SYNC
      val jobConfigType = JobConfigType.SYNC
      val jobStatus = JobStatus.SUCCEEDED
      val apiJobStatus = io.airbyte.api.model.generated.JobStatus.SUCCEEDED
      val jobAggregatedStats =
        JobAggregatedStats()
          .bytesCommitted(bytesCommitted1 + bytesCommitted2)
          .bytesEmitted(bytesEmitted1 + bytesEmitted2)
          .recordsCommitted(recordsCommitted1 + recordsCommitted2)
          .recordsEmitted(recordsEmitted1 + recordsEmitted2)
      val job =
        Job(
          jobId,
          configType,
          connectionId.toString(),
          JobConfig(),
          mutableListOf(),
          jobStatus,
          startedAt.toEpochMilli(),
          createdAt.toEpochMilli(),
          updatedAt.toEpochMilli(),
          true,
        )
      val jobList = listOf(job)
      val jobRead =
        JobRead()
          .id(job.id)
          .configType(jobConfigType)
          .status(apiJobStatus)
          .aggregatedStats(jobAggregatedStats)
          .createdAt(job.createdAtInSecond)
          .updatedAt(job.updatedAtInSecond)
          .startedAt(job.startedAtInSecond)
          .streamAggregatedStats(
            listOf<@Valid StreamStats?>(
              StreamStats()
                .streamName(stream1Name)
                .streamNamespace(stream1Namespace)
                .bytesCommitted(bytesCommitted1)
                .recordsCommitted(recordsCommitted1),
              StreamStats()
                .streamName(stream2Name)
                .streamNamespace(null)
                .bytesCommitted(bytesCommitted2)
                .recordsCommitted(recordsCommitted2),
            ),
          )
      val jobWithAttemptsRead =
        JobWithAttemptsRead()
          .job(jobRead)

      val apiReq =
        ConnectionLastJobPerStreamRequestBody()
          .connectionId(connectionId)

      val stream1ReadItem =
        ConnectionLastJobPerStreamReadItem()
          .streamName(stream1Name)
          .streamNamespace(stream1Namespace)
          .jobId(jobId)
          .configType(jobConfigType)
          .jobStatus(apiJobStatus)
          .bytesCommitted(bytesCommitted1)
          .recordsCommitted(recordsCommitted1)
          .startedAt(startedAt.toEpochMilli())
          .endedAt(updatedAt.toEpochMilli())

      val stream2ReadItem =
        ConnectionLastJobPerStreamReadItem()
          .streamName(stream2Name)
          .streamNamespace(null)
          .jobId(jobId)
          .configType(jobConfigType)
          .jobStatus(apiJobStatus)
          .bytesCommitted(bytesCommitted2)
          .recordsCommitted(recordsCommitted2)
          .startedAt(startedAt.toEpochMilli())
          .endedAt(updatedAt.toEpochMilli())

      whenever(streamStatusesService.getLastJobIdWithStatsByStream(connectionId))
        .thenReturn(
          mapOf(
            stream1Descriptor to jobId,
            stream2Descriptor to jobId,
          ),
        )

      whenever(jobPersistence.listJobsLight(setOf(jobId))).thenReturn(jobList)

      mockStatic(StatsAggregationHelper::class.java).use { mockStatsAggregationHelper ->
        mockStatsAggregationHelper
          .`when`<Any> {
            getJobIdToJobWithAttemptsReadMap(
              eq(jobList),
              eq(jobPersistence),
            )
          }.thenReturn(mapOf(jobId to jobWithAttemptsRead))

        val expectedStream1And2 = listOf(stream1ReadItem, stream2ReadItem)
        assertEquals(expectedStream1And2, connectionsHandler.getConnectionLastJobPerStream(apiReq))
      }
    }
  }

  private val scope = "abc"
  private val prestoToHudi = "presto to hudi"
  private val prestoToHudiPrefix = "presto_to_hudi"
  private val sourceTest = "source-test"
  private val destinationTest = "destination-test"
  private val cursor1 = "cursor1"
  private val cursor2 = "cursor2"
  private val pk1 = "pk1"
  private val pk2 = "pk2"
  private val pk3 = "pk3"
  private val stream1 = "stream1"
  private val stream2 = "stream2"
  private val azkabanUsers = "azkaban_users"
  private val cronTimezoneUtc = "UTC"
  private val timezoneLosAngeles = "America/Los_Angeles"
  private val cronExpression = "0 0 */2 * * ?"
  private val streamSelectionData = "null/users-data0"

  data class MatchingKeyTestCase(
    val testName: String,
    val matchingKeys: List<List<String>>?,
    val primaryKey: List<List<String>>?,
    val expectedException: Class<out Throwable>?,
  )

  companion object {
    @JvmStatic
    fun matchingKeysTestCases() =
      listOf(
        // Test case: No matching keys defined - should pass
        MatchingKeyTestCase(
          testName = "No matching keys",
          matchingKeys = null,
          primaryKey = listOf(listOf("id")),
          expectedException = null,
        ),
        // Test case: Matching keys defined but no primary key - should fail
        MatchingKeyTestCase(
          testName = "Matching keys but no primary key",
          matchingKeys = listOf(listOf("id"), listOf("email")),
          primaryKey = null,
          expectedException = DestinationCatalogMissingPrimaryKeyProblem::class.java,
        ),
        // Test case: Empty primary key with matching keys - should fail
        MatchingKeyTestCase(
          testName = "Matching keys but empty primary key",
          matchingKeys = listOf(listOf("id"), listOf("email")),
          primaryKey = emptyList(),
          expectedException = DestinationCatalogMissingPrimaryKeyProblem::class.java,
        ),
        // Test case: Primary key matches one of the matching keys - should pass
        MatchingKeyTestCase(
          testName = "Primary key matches single matching key",
          matchingKeys = listOf(listOf("id"), listOf("email")),
          primaryKey = listOf(listOf("id")),
          expectedException = null,
        ),
        // Test case: Primary key matches composite matching key - should pass
        MatchingKeyTestCase(
          testName = "Primary key matches composite matching key",
          matchingKeys = listOf(listOf("id"), listOf("company", "email")),
          primaryKey = listOf(listOf("company"), listOf("email")),
          expectedException = null,
        ),
        // Test case: Primary key does not match any matching key - should fail
        MatchingKeyTestCase(
          testName = "Primary key not in matching keys",
          matchingKeys = listOf(listOf("id"), listOf("email")),
          primaryKey = listOf(listOf("name")),
          expectedException = DestinationCatalogInvalidPrimaryKeyProblem::class.java,
        ),
        // Test case: Primary key has different order but same fields as composite matching key - should pass
        MatchingKeyTestCase(
          testName = "Primary key different order than composite matching key",
          matchingKeys = listOf(listOf("id"), listOf("company", "email")),
          primaryKey = listOf(listOf("email"), listOf("company")),
          expectedException = null,
        ),
        // Test case: Primary key is subset of composite matching key - should fail
        MatchingKeyTestCase(
          testName = "Primary key subset of composite matching key",
          matchingKeys = listOf(listOf("id"), listOf("company", "email")),
          primaryKey = listOf(listOf("company")),
          expectedException = DestinationCatalogInvalidPrimaryKeyProblem::class.java,
        ),
        // Test case: Primary key has extra fields compared to matching key - should fail
        MatchingKeyTestCase(
          testName = "Primary key superset of matching key",
          matchingKeys = listOf(listOf("id"), listOf("company")),
          primaryKey = listOf(listOf("id"), listOf("company"), listOf("extra")),
          expectedException = DestinationCatalogInvalidPrimaryKeyProblem::class.java,
        ),
      )
  }
}
