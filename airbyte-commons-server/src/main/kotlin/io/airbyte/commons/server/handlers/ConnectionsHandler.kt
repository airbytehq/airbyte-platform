/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import datadog.trace.api.Trace
import io.airbyte.analytics.TrackingClient
import io.airbyte.api.common.StreamDescriptorUtils.buildFullyQualifiedName
import io.airbyte.api.model.generated.ActorDefinitionRequestBody
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.AttemptRead
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.ConfiguredStreamMapper
import io.airbyte.api.model.generated.ConnectionAutoPropagateResult
import io.airbyte.api.model.generated.ConnectionAutoPropagateSchemaChange
import io.airbyte.api.model.generated.ConnectionContextRead
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionDataHistoryRequestBody
import io.airbyte.api.model.generated.ConnectionEventIdRequestBody
import io.airbyte.api.model.generated.ConnectionEventList
import io.airbyte.api.model.generated.ConnectionEventListMinimal
import io.airbyte.api.model.generated.ConnectionEventMinimal
import io.airbyte.api.model.generated.ConnectionEventType
import io.airbyte.api.model.generated.ConnectionEventWithDetails
import io.airbyte.api.model.generated.ConnectionEventsBackfillRequestBody
import io.airbyte.api.model.generated.ConnectionEventsListMinimalRequestBody
import io.airbyte.api.model.generated.ConnectionEventsRequestBody
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamReadItem
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamRequestBody
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionReadList
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.ConnectionStatusRead
import io.airbyte.api.model.generated.ConnectionStatusesRequestBody
import io.airbyte.api.model.generated.ConnectionStreamHistoryReadItem
import io.airbyte.api.model.generated.ConnectionStreamHistoryRequestBody
import io.airbyte.api.model.generated.ConnectionSyncStatus
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.FailureOrigin
import io.airbyte.api.model.generated.FailureType
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.JobSyncResultRead
import io.airbyte.api.model.generated.JobWithAttemptsRead
import io.airbyte.api.model.generated.ListConnectionsForWorkspacesRequestBody
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.api.model.generated.PostprocessDiscoveredCatalogResult
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.model.generated.StreamStats
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.api.model.generated.Tag
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.problems.model.generated.ProblemConnectionConflictingStreamsData
import io.airbyte.api.problems.model.generated.ProblemConnectionConflictingStreamsDataItem
import io.airbyte.api.problems.model.generated.ProblemConnectionUnsupportedFileTransfersData
import io.airbyte.api.problems.model.generated.ProblemDestinationCatalogAdditionalFieldData
import io.airbyte.api.problems.model.generated.ProblemDestinationCatalogOperationData
import io.airbyte.api.problems.model.generated.ProblemDestinationCatalogRequiredData
import io.airbyte.api.problems.model.generated.ProblemDestinationCatalogRequiredFieldData
import io.airbyte.api.problems.model.generated.ProblemDestinationCatalogStreamData
import io.airbyte.api.problems.model.generated.ProblemMapperErrorData
import io.airbyte.api.problems.model.generated.ProblemMapperErrorDataMapper
import io.airbyte.api.problems.model.generated.ProblemMapperErrorsData
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemStreamDataItem
import io.airbyte.api.problems.throwable.generated.ConnectionConflictingStreamProblem
import io.airbyte.api.problems.throwable.generated.ConnectionDoesNotSupportFileTransfersProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogInvalidAdditionalFieldProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogInvalidOperationProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogMissingObjectNameProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogMissingRequiredFieldProblem
import io.airbyte.api.problems.throwable.generated.DestinationCatalogRequiredProblem
import io.airbyte.api.problems.throwable.generated.MapperValidationProblem
import io.airbyte.api.problems.throwable.generated.StreamDoesNotSupportFileTransfersProblem
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.commons.converters.ApiConverters.Companion.toInternal
import io.airbyte.commons.converters.toServerApi
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.JsonSchemas
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.protocol.CatalogDiffHelpers.getCatalogDiff
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.CatalogDiffConverters.streamTransformToApi
import io.airbyte.commons.server.converters.ConnectionHelper
import io.airbyte.commons.server.converters.ConnectionHelper.Companion.validateCatalogDoesntContainDuplicateStreamNames
import io.airbyte.commons.server.converters.ConnectionHelper.Companion.validateWorkspace
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.converters.JobConverter.Companion.getJobWithAttemptsRead
import io.airbyte.commons.server.converters.JobConverter.Companion.getStreamsAssociatedWithJob
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper.UpdateSchemaResult
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.handlers.helpers.ConnectionScheduleHelper
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.handlers.helpers.ContextBuilder
import io.airbyte.commons.server.handlers.helpers.MapperSecretHelper
import io.airbyte.commons.server.handlers.helpers.NotificationHelper
import io.airbyte.commons.server.handlers.helpers.PaginationHelper.pageSize
import io.airbyte.commons.server.handlers.helpers.PaginationHelper.rowOffset
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper.getJobIdToJobWithAttemptsReadMap
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.commons.server.services.DestinationDiscoverService.Companion.actorCatalogToDestinationCatalog
import io.airbyte.commons.server.validation.CatalogValidator
import io.airbyte.config.ActorCatalogWithUpdatedAt
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.BasicSchedule
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationCatalog
import io.airbyte.config.DestinationOperation
import io.airbyte.config.FailureReason
import io.airbyte.config.Field
import io.airbyte.config.FieldSelectionData
import io.airbyte.config.Job
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.MapperConfig
import io.airbyte.config.Schedule
import io.airbyte.config.ScheduleData
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardSync
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamDescriptorForDestination
import io.airbyte.config.helpers.CatalogHelpers.Companion.configuredCatalogToCatalog
import io.airbyte.config.helpers.ScheduleHelpers.getIntervalInSecond
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.StatePersistence
import io.airbyte.config.persistence.StreamGenerationRepository
import io.airbyte.config.persistence.helper.CatalogGenerationSetter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.repositories.entities.ConnectionTimelineEventMinimal
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.StreamStatusesService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ConnectionAutoDisabledReason
import io.airbyte.data.services.shared.ConnectionAutoUpdatedReason
import io.airbyte.data.services.shared.ConnectionEvent
import io.airbyte.data.services.shared.FailedEvent
import io.airbyte.data.services.shared.FinalStatusEvent
import io.airbyte.featureflag.CheckWithCatalog
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.EnableDestinationCatalogValidation
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.ResetStreamsStateWhenDisabled
import io.airbyte.featureflag.ValidateConflictingDestinationStreams
import io.airbyte.featureflag.Workspace
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.mappers.transformations.DestinationCatalogGenerator.MapperError
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceConstants
import io.airbyte.metrics.lib.ApmTraceUtils.addExceptionToTrace
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import jakarta.validation.Valid
import java.io.IOException
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.NavigableMap
import java.util.Objects
import java.util.Optional
import java.util.TreeMap
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer
import java.util.function.Consumer
import java.util.function.Supplier
import java.util.stream.Collectors

/**
 * ConnectionsHandler. Javadocs suppressed because api docs should be used as source of truth.
 *
 */
@Singleton
@Deprecated("New connection-related functionality should be added to the ConnectionService")
class ConnectionsHandler // TODO: Worth considering how we might refactor this. The arguments list feels a little long.
  @Inject
  constructor(
    private val streamRefreshesHandler: StreamRefreshesHandler,
    private val jobPersistence: JobPersistence,
    private val catalogService: CatalogService,
    @param:Named("uuidGenerator") private val uuidGenerator: Supplier<UUID>,
    private val workspaceHelper: WorkspaceHelper,
    private val trackingClient: TrackingClient,
    private val eventRunner: EventRunner,
    private val connectionHelper: ConnectionHelper,
    private val featureFlagClient: FeatureFlagClient,
    private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
    private val connectorSpecHandler: ConnectorDefinitionSpecificationHandler,
    private val streamGenerationRepository: StreamGenerationRepository,
    private val catalogGenerationSetter: CatalogGenerationSetter,
    private val catalogValidator: CatalogValidator,
    private val notificationHelper: NotificationHelper,
    private val streamStatusesService: StreamStatusesService,
    private val connectionTimelineEventService: ConnectionTimelineEventService,
    private val connectionTimelineEventHelper: ConnectionTimelineEventHelper,
    private val statePersistence: StatePersistence,
    private val sourceService: SourceService,
    private val destinationService: DestinationService,
    private val connectionService: ConnectionService,
    private val workspaceService: WorkspaceService,
    private val destinationCatalogGenerator: DestinationCatalogGenerator,
    private val catalogConverter: CatalogConverter,
    private val applySchemaChangeHelper: ApplySchemaChangeHelper,
    private val apiPojoConverters: ApiPojoConverters,
    private val connectionScheduleHelper: ConnectionScheduleHelper,
    private val mapperSecretHelper: MapperSecretHelper,
    private val metricClient: MetricClient,
    private val licenseEntitlementChecker: LicenseEntitlementChecker,
    private val contextBuilder: ContextBuilder,
  ) {
    private val maxJobLookback = 10

    /**
     * Modifies the given StandardSync by applying changes from a partially-filled ConnectionUpdate
     * patch. Any fields that are null in the patch will be left unchanged.
     */
    @VisibleForTesting
    @Throws(JsonValidationException::class, ConfigNotFoundException::class)
    fun applyPatchToStandardSync(
      sync: StandardSync,
      patch: ConnectionUpdate,
      workspaceId: UUID,
    ) {
      // update the sync's schedule using the patch's scheduleType and scheduleData. validations occur in
      // the helper to ensure both fields
      // make sense together.
      if (patch.scheduleType != null) {
        connectionScheduleHelper.populateSyncFromScheduleTypeAndData(sync, patch.scheduleType, patch.scheduleData)
      }

      // the rest of the fields are straightforward to patch. If present in the patch, set the field to
      // the value
      // in the patch. Otherwise, leave the field unchanged.
      if (patch.syncCatalog != null) {
        validateCatalogDoesntContainDuplicateStreamNames(patch.syncCatalog)
        validateCatalogSize(patch.syncCatalog, workspaceId, "update")

        assignIdsToIncomingMappers(patch.syncCatalog)
        val configuredCatalog = catalogConverter.toConfiguredInternal(patch.syncCatalog)
        validateConfiguredMappers(configuredCatalog)

        val configuredCatalogNoSecrets =
          mapperSecretHelper.updateAndReplaceMapperSecrets(workspaceId, sync.catalog, configuredCatalog)
        sync.catalog = configuredCatalogNoSecrets
        sync.withFieldSelectionData(catalogConverter.getFieldSelectionData(patch.syncCatalog))
      }

      if (patch.name != null) {
        sync.name = patch.name
      }

      if (patch.namespaceDefinition != null) {
        sync.namespaceDefinition = patch.namespaceDefinition?.convertTo<JobSyncConfig.NamespaceDefinitionType>()
      }

      if (patch.namespaceFormat != null) {
        sync.namespaceFormat = patch.namespaceFormat
      }

      if (patch.prefix != null) {
        sync.prefix = patch.prefix
      }

      if (patch.operationIds != null) {
        sync.operationIds = patch.operationIds
      }

      if (patch.status != null) {
        sync.status = apiPojoConverters.toPersistenceStatus(patch.status)
      }

      if (patch.sourceCatalogId != null) {
        sync.sourceCatalogId = patch.sourceCatalogId
      }

      if (patch.destinationCatalogId != null) {
        sync.destinationCatalogId = patch.destinationCatalogId
      }

      if (patch.resourceRequirements != null) {
        sync.resourceRequirements = apiPojoConverters.resourceRequirementsToInternal(patch.resourceRequirements)
      }

      if (patch.breakingChange != null) {
        sync.breakingChange = patch.breakingChange
      }

      if (patch.notifySchemaChanges != null) {
        sync.notifySchemaChanges = patch.notifySchemaChanges
      }

      if (patch.notifySchemaChangesByEmail != null) {
        sync.notifySchemaChangesByEmail = patch.notifySchemaChangesByEmail
      }

      if (patch.nonBreakingChangesPreference != null) {
        sync.nonBreakingChangesPreference = apiPojoConverters.toPersistenceNonBreakingChangesPreference(patch.nonBreakingChangesPreference)
      }

      if (patch.backfillPreference != null) {
        sync.backfillPreference = apiPojoConverters.toPersistenceBackfillPreference(patch.backfillPreference)
      }

      if (patch.tags != null) {
        sync.tags =
          patch.tags
            .stream()
            .map { tag: Tag? ->
              apiPojoConverters.toInternalTag(
                tag!!,
              )
            }.toList()
      }
    }

    private fun assignIdsToIncomingMappers(catalog: AirbyteCatalog) {
      catalog.streams.forEach(
        Consumer { stream: AirbyteStreamAndConfiguration ->
          stream.config.mappers.forEach(
            Consumer { mapper: ConfiguredStreamMapper ->
              if (mapper.id == null) {
                mapper.id = uuidGenerator.get()
              }
            },
          )
        },
      )
    }

    private fun validateConfiguredMappers(configuredCatalog: ConfiguredAirbyteCatalog) {
      val result = destinationCatalogGenerator.generateDestinationCatalog(configuredCatalog)
      if (result.errors.isEmpty()) {
        return
      }

      val errors =
        result.errors.entries
          .stream()
          .flatMap { streamErrors: Map.Entry<StreamDescriptor, Map<MapperConfig, MapperError>> ->
            streamErrors.value.entries.stream().map { mapperError: Map.Entry<MapperConfig, MapperError> ->
              ProblemMapperErrorData()
                .stream(streamErrors.key.name)
                .error(mapperError.value.type.name)
                .mapper(
                  ProblemMapperErrorDataMapper()
                    .id(mapperError.key.id())
                    .type(mapperError.key.name())
                    .mapperConfiguration(mapperError.key.config()),
                )
            }
          }.toList()

      if (!errors.isEmpty()) {
        throw MapperValidationProblem(ProblemMapperErrorsData().errors(errors))
      }
    }

    private fun generateDestinationStreamKey(
      namespaceDefinition: String?,
      namespaceFormat: String?,
      prefix: String?,
      streamNamespace: String?,
      streamName: String,
    ): String = namespaceDefinition + namespaceFormat + streamNamespace + prefix + streamName

    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    private fun validateStreamsDoNotConflictWithExistingDestinationStreams(
      newCatalog: AirbyteCatalog,
      destinationId: UUID,
      namespaceDefinitionType: String?,
      namespaceFormat: String?,
      prefix: String?,
      currentConnectionId: UUID?,
    ) {
      val workspaceId = workspaceHelper.getWorkspaceForDestinationId(destinationId)
      val organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId)

      addTagsToTrace(
        java.util.List.of(
          MetricAttribute(ApmTraceConstants.Tags.ORGANIZATION_ID_KEY, organizationId.toString()),
          MetricAttribute(ApmTraceConstants.Tags.DESTINATION_ID_KEY, destinationId.toString()),
        ),
      )

      val existingStreams = connectionService.listStreamsForDestination(destinationId, currentConnectionId)

      // Create map of existing streams once, filtering out streams from the current connection if
      // specified
      val existingStreamMap =
        existingStreams
          .stream()
          .collect(
            Collectors.toMap(
              { existingStream: StreamDescriptorForDestination ->
                generateDestinationStreamKey(
                  existingStream.namespaceDefinition.toString(),
                  existingStream.namespaceFormat,
                  existingStream.prefix,
                  existingStream.streamNamespace,
                  existingStream.streamName,
                )
              },
              { stream: StreamDescriptorForDestination? -> stream },
              { existing: StreamDescriptorForDestination?, replacement: StreamDescriptorForDestination? -> existing },
            ),
          )

      // Get only selected streams from the catalog
      val selectedStreams =
        newCatalog.streams
          .stream()
          .filter { s: AirbyteStreamAndConfiguration -> s.config.selected }
          .toList()

      // Process all selected streams
      val conflictingStreams =
        selectedStreams
          .stream()
          .map { streamAndConfig: AirbyteStreamAndConfiguration ->
            val key =
              generateDestinationStreamKey(
                namespaceDefinitionType,
                namespaceFormat,
                prefix,
                streamAndConfig.stream.namespace,
                streamAndConfig.stream.name,
              )
            existingStreamMap.getOrDefault(key, null)
          }.filter { obj: StreamDescriptorForDestination? -> Objects.nonNull(obj) }
          .toList()

      // If any conflicts found, throw exception
      if (!conflictingStreams.isEmpty()) {
        val streams =
          conflictingStreams
            .stream()
            .map { stream: StreamDescriptorForDestination? ->
              ProblemConnectionConflictingStreamsDataItem()
                .connectionIds(stream!!.connectionIds)
                .streamName(stream.streamName)
                .streamNamespace(stream.streamNamespace)
            }.toList()

        throw ConnectionConflictingStreamProblem(ProblemConnectionConflictingStreamsData().streams(streams))
      }
    }

    @Throws(
      JsonValidationException::class,
      IOException::class,
      ConfigNotFoundException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun createConnection(connectionCreate: ConnectionCreate): ConnectionRead {
      // Validate source and destination

      val sourceConnection = sourceService.getSourceConnection(connectionCreate.sourceId)
      val destinationConnection = destinationService.getDestinationConnection(connectionCreate.destinationId)

      // Set this as default name if connectionCreate doesn't have it
      val defaultName = sourceConnection.name + " <> " + destinationConnection.name

      val operationIds = if (connectionCreate.operationIds != null) connectionCreate.operationIds else emptyList()

      validateWorkspace(
        workspaceHelper,
        connectionCreate.sourceId,
        connectionCreate.destinationId,
        operationIds,
      )

      val workspaceId = workspaceHelper.getWorkspaceForDestinationId(connectionCreate.destinationId)
      val organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId)

      // Ensure org is entitled to use source and destination
      licenseEntitlementChecker.ensureEntitled(organizationId, Entitlement.SOURCE_CONNECTOR, sourceConnection.sourceDefinitionId)
      licenseEntitlementChecker.ensureEntitled(organizationId, Entitlement.DESTINATION_CONNECTOR, destinationConnection.destinationDefinitionId)

      val connectionId = uuidGenerator.get()

      // If not specified, default the NamespaceDefinition to 'source'
      val namespaceDefinitionType =
        if (connectionCreate.namespaceDefinition == null) {
          JobSyncConfig.NamespaceDefinitionType.SOURCE
        } else {
          connectionCreate.namespaceDefinition?.convertTo<JobSyncConfig.NamespaceDefinitionType>()
        }

      // persist sync
      val standardSync =
        StandardSync()
          .withConnectionId(connectionId)
          .withName(if (connectionCreate.name != null) connectionCreate.name else defaultName)
          .withNamespaceDefinition(namespaceDefinitionType)
          .withNamespaceFormat(connectionCreate.namespaceFormat)
          .withPrefix(connectionCreate.prefix)
          .withSourceId(connectionCreate.sourceId)
          .withDestinationId(connectionCreate.destinationId)
          .withOperationIds(operationIds)
          .withStatus(apiPojoConverters.toPersistenceStatus(connectionCreate.status))
          .withSourceCatalogId(connectionCreate.sourceCatalogId)
          .withDestinationCatalogId(connectionCreate.destinationCatalogId)
          .withBreakingChange(false)
          .withNotifySchemaChanges(connectionCreate.notifySchemaChanges)
          .withNonBreakingChangesPreference(
            apiPojoConverters.toPersistenceNonBreakingChangesPreference(connectionCreate.nonBreakingChangesPreference),
          ).withBackfillPreference(apiPojoConverters.toPersistenceBackfillPreference(connectionCreate.backfillPreference))
          .withTags(
            connectionCreate.tags
              .stream()
              .map { tag: Tag? ->
                apiPojoConverters.toInternalTag(
                  tag!!,
                )
              }.toList(),
          )
      if (connectionCreate.resourceRequirements != null) {
        standardSync.withResourceRequirements(apiPojoConverters.resourceRequirementsToInternal(connectionCreate.resourceRequirements))
      }

      // TODO Undesirable behavior: sending a null configured catalog should not be valid?
      if (connectionCreate.syncCatalog != null) {
        val sourceDefinition =
          sourceService
            .getStandardSourceDefinition(sourceConnection.sourceDefinitionId)
        val sourceVersion =
          actorDefinitionVersionHelper
            .getSourceVersion(sourceDefinition, sourceConnection.workspaceId, sourceConnection.sourceId)
        val destinationDefinition =
          destinationService
            .getStandardDestinationDefinition(destinationConnection.destinationDefinitionId)
        val destinationVersion =
          actorDefinitionVersionHelper
            .getDestinationVersion(destinationDefinition, destinationConnection.workspaceId, destinationConnection.destinationId)

        validateCatalogIncludeFiles(connectionCreate.syncCatalog, sourceVersion, destinationVersion)
        validateCatalogDoesntContainDuplicateStreamNames(connectionCreate.syncCatalog)
        validateCatalogSize(connectionCreate.syncCatalog, workspaceId, "create")

        if (featureFlagClient.boolVariation(ValidateConflictingDestinationStreams, Organization(organizationId))) {
          validateStreamsDoNotConflictWithExistingDestinationStreams(
            connectionCreate.syncCatalog,
            connectionCreate.destinationId,
            connectionCreate.namespaceDefinition?.toString(),
            connectionCreate.namespaceFormat,
            connectionCreate.prefix,
            null,
          )
        }

        if (featureFlagClient.boolVariation(EnableDestinationCatalogValidation, Workspace(workspaceId))) {
          val hasDestinationCatalog = connectionCreate.destinationCatalogId != null
          if (destinationVersion.supportsDataActivation && !hasDestinationCatalog) {
            throw DestinationCatalogRequiredProblem(
              ProblemDestinationCatalogRequiredData()
                .destinationId(connectionCreate.destinationId),
            )
          }

          if (hasDestinationCatalog) {
            val destinationCatalog =
              actorCatalogToDestinationCatalog(catalogService.getActorCatalogById(connectionCreate.destinationCatalogId)).catalog
            validateCatalogWithDestinationCatalog(connectionCreate.syncCatalog, destinationCatalog)
          }
        }

        applyDefaultIncludeFiles(connectionCreate.syncCatalog, sourceVersion, destinationVersion)
        assignIdsToIncomingMappers(connectionCreate.syncCatalog)
        val configuredCatalog =
          catalogConverter.toConfiguredInternal(connectionCreate.syncCatalog)
        validateConfiguredMappers(configuredCatalog)

        val configuredCatalogNoSecrets = mapperSecretHelper.createAndReplaceMapperSecrets(workspaceId, configuredCatalog)
        standardSync.withCatalog(configuredCatalogNoSecrets)
        standardSync.withFieldSelectionData(catalogConverter.getFieldSelectionData(connectionCreate.syncCatalog))
      } else {
        standardSync.withCatalog(ConfiguredAirbyteCatalog().withStreams(emptyList()))
        standardSync.withFieldSelectionData(FieldSelectionData())
      }

      if (connectionCreate.schedule != null && connectionCreate.scheduleType != null) {
        throw JsonValidationException("supply old or new schedule schema but not both")
      }

      if (connectionCreate.scheduleType != null) {
        connectionScheduleHelper.populateSyncFromScheduleTypeAndData(
          standardSync,
          connectionCreate.scheduleType,
          connectionCreate.scheduleData,
        )
      } else {
        populateSyncFromLegacySchedule(standardSync, connectionCreate)
      }
      if (workspaceId != null && featureFlagClient.boolVariation(CheckWithCatalog, Workspace(workspaceId))) {
        // TODO this is the hook for future check with catalog work
        log.info { "Entered into Dark Launch Code for Check with Catalog for connectionId $connectionId" }
      }
      connectionService.writeStandardSync(standardSync)

      trackNewConnection(standardSync)

      try {
        log.info { "Starting a connection manager workflow for connectionId $connectionId" }
        eventRunner.createConnectionManagerWorkflow(connectionId)
      } catch (e: Exception) {
        log.error(e) { "Start of the connection manager workflow failed; deleting connectionId $connectionId" }
        // deprecate the newly created connection and also delete the newly created workflow.
        deleteConnection(connectionId)
        throw e
      }

      return buildConnectionRead(connectionId)
    }

    private fun populateSyncFromLegacySchedule(
      standardSync: StandardSync,
      connectionCreate: ConnectionCreate,
    ) {
      if (connectionCreate.schedule != null) {
        val schedule =
          Schedule()
            .withTimeUnit(apiPojoConverters.toPersistenceTimeUnit(connectionCreate.schedule.timeUnit))
            .withUnits(connectionCreate.schedule.units)
        // Populate the legacy field.
        // TODO(https://github.com/airbytehq/airbyte/issues/11432): remove.
        standardSync
          .withManual(false)
          .withSchedule(schedule)
        // Also write into the new field. This one will be consumed if populated.
        standardSync
          .withScheduleType(StandardSync.ScheduleType.BASIC_SCHEDULE)
        standardSync.withScheduleData(
          ScheduleData().withBasicSchedule(
            BasicSchedule()
              .withTimeUnit(apiPojoConverters.toBasicScheduleTimeUnit(connectionCreate.schedule.timeUnit))
              .withUnits(connectionCreate.schedule.units),
          ),
        )
      } else {
        standardSync.withManual(true)
        standardSync.withScheduleType(StandardSync.ScheduleType.MANUAL)
      }
    }

    private fun trackNewConnection(standardSync: StandardSync) {
      try {
        val workspaceId = workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(standardSync.connectionId)
        val metadataBuilder = generateMetadata(standardSync)
        trackingClient.track(workspaceId, ScopeType.WORKSPACE, "New Connection - Backend", metadataBuilder.build())
      } catch (e: Exception) {
        log.error(e) { "failed while reporting usage." }
      }
    }

    private fun trackUpdateConnection(standardSync: StandardSync) {
      try {
        val workspaceId = workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(standardSync.connectionId)
        val metadataBuilder = generateMetadata(standardSync)
        trackingClient.track(workspaceId, ScopeType.WORKSPACE, "Updated Connection - Backend", metadataBuilder.build())
      } catch (e: Exception) {
        log.error(e) { "failed while reporting usage." }
      }
    }

    private fun generateMetadata(standardSync: StandardSync): ImmutableMap.Builder<String, Any> {
      val metadata = ImmutableMap.builder<String, Any>()

      val connectionId = standardSync.connectionId
      val sourceDefinition =
        sourceService
          .getSourceDefinitionFromConnection(connectionId)
      val destinationDefinition =
        destinationService
          .getDestinationDefinitionFromConnection(connectionId)

      metadata.put("connector_source", sourceDefinition.name)
      metadata.put("connector_source_definition_id", sourceDefinition.sourceDefinitionId)
      metadata.put("connector_destination", destinationDefinition.name)
      metadata.put("connector_destination_definition_id", destinationDefinition.destinationDefinitionId)
      metadata.put("connection_id", standardSync.connectionId)
      metadata.put("source_id", standardSync.sourceId)
      metadata.put("destination_id", standardSync.destinationId)

      val frequencyString: String
      if (standardSync.scheduleType != null && standardSync.scheduleData != null) {
        frequencyString = getFrequencyStringFromScheduleType(standardSync.scheduleType, standardSync.scheduleData)
      } else if (standardSync.manual) {
        frequencyString = "manual"
      } else {
        val intervalInMinutes = TimeUnit.SECONDS.toMinutes(getIntervalInSecond(standardSync.schedule))
        frequencyString = "$intervalInMinutes min"
      }
      var fieldSelectionEnabled = false
      if (standardSync.fieldSelectionData != null && standardSync.fieldSelectionData.additionalProperties != null) {
        fieldSelectionEnabled =
          standardSync.fieldSelectionData.additionalProperties
            .entries
            .stream()
            .anyMatch { obj: Map.Entry<String?, Boolean?> -> obj.value!! }
      }
      metadata.put("field_selection_active", fieldSelectionEnabled)
      metadata.put("frequency", frequencyString)
      return metadata
    }

    private fun isPatchRelevantForDestinationValidation(connectionPatch: ConnectionUpdate): Boolean =
      connectionPatch.syncCatalog != null ||
        connectionPatch.namespaceDefinition != null ||
        connectionPatch.namespaceFormat != null ||
        connectionPatch.prefix != null

    @Throws(
      ConfigNotFoundException::class,
      IOException::class,
      JsonValidationException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun updateConnection(
      connectionPatch: ConnectionUpdate,
      updateReason: String?,
      autoUpdate: Boolean,
    ): ConnectionRead {
      val connectionId = connectionPatch.connectionId
      val workspaceId = workspaceHelper.getWorkspaceForConnectionId(connectionId)

      log.debug { "Starting updateConnection for connectionId {}, workspaceId $connectionId, workspaceId..." }
      log.debug { "incoming connectionPatch: $connectionPatch" }

      val sync = connectionService.getStandardSync(connectionId)
      log.debug { "initial StandardSync: $sync" }

      // Ensure org is entitled to use source and destination
      val sourceDefinition = sourceService.getSourceDefinitionFromConnection(sync.connectionId)
      val destinationDefinition = destinationService.getDestinationDefinitionFromConnection(sync.connectionId)
      val organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId)
      licenseEntitlementChecker.ensureEntitled(organizationId, Entitlement.SOURCE_CONNECTOR, sourceDefinition.sourceDefinitionId)
      licenseEntitlementChecker.ensureEntitled(organizationId, Entitlement.DESTINATION_CONNECTOR, destinationDefinition.destinationDefinitionId)

      if (connectionPatch.syncCatalog != null) {
        val sourceVersion =
          actorDefinitionVersionHelper
            .getSourceVersion(sourceDefinition, workspaceId, sync.sourceId)
        val destinationVersion =
          actorDefinitionVersionHelper
            .getDestinationVersion(destinationDefinition, workspaceId, sync.destinationId)
        validateCatalogIncludeFiles(connectionPatch.syncCatalog, sourceVersion, destinationVersion)
        applyDefaultIncludeFiles(connectionPatch.syncCatalog, sourceVersion, destinationVersion)

        if (featureFlagClient.boolVariation(
            EnableDestinationCatalogValidation,
            Multi(java.util.List.of<Context>(Workspace(workspaceId), Connection(sync.connectionId))),
          )
        ) {
          val destCatalogId =
            if (connectionPatch.destinationCatalogId != null) {
              connectionPatch.destinationCatalogId
            } else {
              sync.destinationCatalogId
            }
          val hasDestinationCatalog = destCatalogId != null

          if (destinationVersion.supportsDataActivation && !hasDestinationCatalog) {
            throw DestinationCatalogRequiredProblem(
              ProblemDestinationCatalogRequiredData()
                .destinationId(sync.destinationId),
            )
          }

          if (hasDestinationCatalog) {
            val destinationCatalog =
              actorCatalogToDestinationCatalog(catalogService.getActorCatalogById(destCatalogId!!)).catalog
            validateCatalogWithDestinationCatalog(connectionPatch.syncCatalog, destinationCatalog)
          }
        }
      }

      if (isPatchRelevantForDestinationValidation(connectionPatch) &&
        (updateReason != ConnectionAutoUpdatedReason.SCHEMA_CHANGE_AUTO_PROPAGATE.name) &&
        featureFlagClient.boolVariation(
          ValidateConflictingDestinationStreams,
          Organization(organizationId),
        )
      ) {
        validateStreamsDoNotConflictWithExistingDestinationStreams(
          if (connectionPatch.syncCatalog != null) connectionPatch.syncCatalog else catalogConverter.toApi(sync.catalog, null),
          sync.destinationId,
          if (connectionPatch.namespaceDefinition != null) {
            connectionPatch.namespaceDefinition.toString()
          } else {
            sync.namespaceDefinition.toString()
          },
          if (connectionPatch.namespaceFormat != null) connectionPatch.namespaceFormat else sync.namespaceFormat,
          if (connectionPatch.prefix != null) connectionPatch.prefix else sync.prefix,
          connectionPatch.connectionId,
        )
      }

      validateConnectionPatch(workspaceHelper, sync, connectionPatch)

      val initialConnectionRead = apiPojoConverters.internalToConnectionRead(sync)
      log.debug { "initial ConnectionRead: $initialConnectionRead" }

      if (connectionPatch.syncCatalog != null &&
        featureFlagClient.boolVariation(ResetStreamsStateWhenDisabled, Workspace(workspaceId))
      ) {
        val newCatalogActiveStream =
          connectionPatch.syncCatalog.streams
            .stream()
            .filter { s: AirbyteStreamAndConfiguration -> s.config.selected }
            .map { s: AirbyteStreamAndConfiguration ->
              io.airbyte.api.model.generated
                .StreamDescriptor()
                .namespace(s.stream.namespace)
                .name(s.stream.name)
            }.toList()
        val deactivatedStreams =
          sync.catalog.streams
            .stream()
            .map { s: ConfiguredAirbyteStream ->
              io.airbyte.api.model.generated
                .StreamDescriptor()
                .name(s.stream.name)
                .namespace(s.stream.namespace)
            }.collect(Collectors.toSet())
        newCatalogActiveStream.forEach(Consumer { o: io.airbyte.api.model.generated.StreamDescriptor -> deactivatedStreams.remove(o) })
        log.debug(
          "Wiping out the state of deactivated streams: [{}]",
          java.lang.String.join(
            ", ",
            deactivatedStreams
              .stream()
              .map { obj: io.airbyte.api.model.generated.StreamDescriptor -> buildFullyQualifiedName(obj) }
              .toList(),
          ),
        )
        statePersistence.bulkDelete(
          connectionId,
          deactivatedStreams
            .stream()
            .map { obj: io.airbyte.api.model.generated.StreamDescriptor -> obj.toInternal() }
            .collect(
              Collectors.toSet(),
            ),
        )
      }

      deleteStateForStreamsWithSyncModeChanged(connectionPatch, sync, connectionId)

      applyPatchToStandardSync(sync, connectionPatch, workspaceId)

      log.debug { "patched StandardSync before persisting: $sync" }
      connectionService.writeStandardSync(sync)

      eventRunner.update(connectionId)

      val updatedRead = buildConnectionRead(connectionId)
      log.debug { "final connectionRead: $updatedRead" }

      trackUpdateConnection(sync)

      // Log settings change event in connection timeline.
      connectionTimelineEventHelper.logConnectionSettingsChangedEventInConnectionTimeline(
        connectionId,
        initialConnectionRead,
        connectionPatch,
        updateReason,
        autoUpdate,
      )

      return updatedRead
    }

    @Throws(IOException::class)
    private fun deleteStateForStreamsWithSyncModeChanged(
      connectionPatch: ConnectionUpdate,
      sync: StandardSync,
      connectionId: UUID,
    ) {
      if (connectionPatch.syncCatalog != null) {
        validateCatalogDoesntContainDuplicateStreamNames(connectionPatch.syncCatalog)
        val activeStreams =
          connectionPatch.syncCatalog.streams
            .stream()
            .filter { streamAndConfig: AirbyteStreamAndConfiguration -> streamAndConfig.config != null && streamAndConfig.config.selected }
            .collect(
              Collectors.toMap<@Valid AirbyteStreamAndConfiguration?, io.airbyte.api.model.generated.StreamDescriptor, SyncMode>(
                { streamAndConfig: AirbyteStreamAndConfiguration? ->
                  io.airbyte.api.model.generated
                    .StreamDescriptor()
                    .name(
                      streamAndConfig!!.stream.name,
                    ).namespace(streamAndConfig.stream.namespace)
                },
                { streamAndConfig: AirbyteStreamAndConfiguration? -> streamAndConfig!!.config.syncMode },
              ),
            )

        val existingStreams =
          sync.catalog.streams
            .stream()
            .collect(
              Collectors.toMap(
                { streamAndConfig: ConfiguredAirbyteStream ->
                  io.airbyte.api.model.generated
                    .StreamDescriptor()
                    .name(streamAndConfig.stream.name)
                    .namespace(streamAndConfig.stream.namespace)
                },
                { streamAndConfig: ConfiguredAirbyteStream ->
                  streamAndConfig.syncMode?.convertTo<SyncMode>()
                },
              ),
            )
        val streamsWithChangedSyncMode =
          activeStreams.entries
            .stream()
            .filter { entry: Map.Entry<io.airbyte.api.model.generated.StreamDescriptor, SyncMode> ->
              val streamDescriptor = entry.key
              val newSyncMode = entry.value
              val existingSyncMode = existingStreams[streamDescriptor]
              existingSyncMode != null && newSyncMode != existingSyncMode
            }.map { obj: Map.Entry<io.airbyte.api.model.generated.StreamDescriptor, SyncMode> -> obj.key }
            .collect(Collectors.toSet())

        if (!streamsWithChangedSyncMode.isEmpty()) {
          statePersistence.bulkDelete(
            connectionId,
            streamsWithChangedSyncMode
              .stream()
              .map<StreamDescriptor> { obj: io.airbyte.api.model.generated.StreamDescriptor -> obj.toInternal() }
              .collect(
                Collectors.toSet(),
              ),
          )
        }
      }
    }

    private fun validateCatalogIncludeFiles(
      newCatalog: AirbyteCatalog,
      sourceVersion: ActorDefinitionVersion,
      destinationVersion: ActorDefinitionVersion,
    ) {
      val enabledStreamsIncludingFiles =
        newCatalog.streams
          .stream()
          .filter { s: AirbyteStreamAndConfiguration ->
            s.config.selected &&
              s.config.includeFiles != null &&
              s.config.includeFiles
          }.toList()

      if (enabledStreamsIncludingFiles.isEmpty()) {
        // No file streams are enabled, so we don't need to do any validation.
        return
      }

      // If either source or destination doesn't support files, we can't enable file transfers
      if (!sourceVersion.supportsFileTransfer || !destinationVersion.supportsFileTransfer) {
        val problemStreams =
          enabledStreamsIncludingFiles
            .stream()
            .map { stream: AirbyteStreamAndConfiguration ->
              ProblemStreamDataItem()
                .streamName(stream.stream.name)
                .streamNamespace(stream.stream.namespace)
            }.toList()
        throw ConnectionDoesNotSupportFileTransfersProblem(ProblemConnectionUnsupportedFileTransfersData().streams(problemStreams))
      }

      // If the stream isn't file based, we can't enable file transfers for that stream
      for (stream in enabledStreamsIncludingFiles) {
        if (stream.stream.isFileBased == null || !stream.stream.isFileBased) {
          throw StreamDoesNotSupportFileTransfersProblem(
            ProblemConnectionUnsupportedFileTransfersData().addstreamsItem(
              ProblemStreamDataItem()
                .streamName(stream.stream.name)
                .streamNamespace(stream.stream.namespace),
            ),
          )
        }
      }
    }

    @VisibleForTesting
    @Throws(JsonValidationException::class)
    fun validateCatalogWithDestinationCatalog(
      catalog: AirbyteCatalog,
      destinationCatalog: DestinationCatalog,
    ) {
      val destinationOperations = destinationCatalog.operations

      // Apply mappers and use generated catalog for validation
      val configuredCatalog = catalogConverter.toConfiguredInternal(catalog)
      val result = destinationCatalogGenerator.generateDestinationCatalog(configuredCatalog)
      val configuredStreams = result.catalog.streams

      for ((stream, _, destinationSyncMode, _, _, _, _, _, fields, _, _, destinationObjectName) in configuredStreams) {
        val configuredObjectName =
          destinationObjectName
            ?: throw DestinationCatalogMissingObjectNameProblem(
              ProblemDestinationCatalogStreamData()
                .streamName(stream.name)
                .streamNamespace(stream.namespace),
            )

        val destinationOperation =
          destinationOperations
            .stream()
            .filter { op: DestinationOperation -> op.objectName == configuredObjectName && op.syncMode == destinationSyncMode }
            .findFirst()
            .orElseThrow {
              DestinationCatalogInvalidOperationProblem(
                ProblemDestinationCatalogOperationData()
                  .streamName(stream.name)
                  .streamNamespace(stream.namespace)
                  .destinationObjectName(configuredObjectName)
                  .syncMode(destinationSyncMode.toString()),
              )
            }

        val destinationFields = destinationOperation.getFields()

        // Required fields must be present
        val requiredFields = destinationFields.stream().filter(Field::required).toList()
        requiredFields.forEach(
          Consumer { field: Field ->
            if (fields!!.stream().noneMatch { f: Field -> f.name == field.name }) {
              throw DestinationCatalogMissingRequiredFieldProblem(
                ProblemDestinationCatalogRequiredFieldData()
                  .streamName(stream.name)
                  .streamNamespace(stream.namespace)
                  .fieldName(field.name)
                  .destinationOperationName(destinationOperation.objectName),
              )
            }
          },
        )

        // Check if schema allows additional properties
        val schemaNode = destinationOperation.jsonSchema
        val allowsAdditionalProperties = JsonSchemas.allowsAdditionalProperties(schemaNode)

        // If additional properties are not allowed, ensure all source fields are present in destination
        if (!allowsAdditionalProperties) {
          val destinationFieldNames = destinationFields.stream().map(Field::name).toList()
          fields!!.forEach(
            Consumer { sourceField: Field ->
              if (!destinationFieldNames.contains(sourceField.name)) {
                throw DestinationCatalogInvalidAdditionalFieldProblem(
                  ProblemDestinationCatalogAdditionalFieldData()
                    .streamName(stream.name)
                    .streamNamespace(stream.namespace)
                    .fieldName(sourceField.name)
                    .destinationOperationName(destinationOperation.objectName),
                )
              }
            },
          )
        }
      }
    }

    private fun validateConnectionPatch(
      workspaceHelper: WorkspaceHelper,
      persistedSync: StandardSync,
      patch: ConnectionUpdate,
    ) {
      // sanity check that we're updating the right connection
      Preconditions.checkArgument(persistedSync.connectionId == patch.connectionId)

      // make sure all operationIds belong to the same workspace as the connection
      validateWorkspace(
        workspaceHelper,
        persistedSync.sourceId,
        persistedSync.destinationId,
        patch.operationIds,
      )

      // make sure the incoming schedule update is sensible. Note that schedule details are further
      // validated in ConnectionScheduleHelper, this just
      // sanity checks that fields are populated when they should be.
      Preconditions.checkArgument(
        patch.schedule == null,
        "ConnectionUpdate should only make changes to the schedule by setting scheduleType and scheduleData. 'schedule' is no longer supported.",
      )

      if (patch.scheduleType == null) {
        Preconditions.checkArgument(
          patch.scheduleData == null,
          "ConnectionUpdate should not include any scheduleData without also specifying a valid scheduleType.",
        )
      } else {
        when (patch.scheduleType) {
          ConnectionScheduleType.MANUAL ->
            Preconditions.checkArgument(
              patch.scheduleData == null,
              "ConnectionUpdate should not include any scheduleData when setting the Connection scheduleType to MANUAL.",
            )

          ConnectionScheduleType.BASIC ->
            Preconditions.checkArgument(
              patch.scheduleData != null,
              "ConnectionUpdate should include scheduleData when setting the Connection scheduleType to BASIC.",
            )

          ConnectionScheduleType.CRON ->
            Preconditions.checkArgument(
              patch.scheduleData != null,
              "ConnectionUpdate should include scheduleData when setting the Connection scheduleType to CRON.",
            )

          else -> throw RuntimeException("Unrecognized scheduleType!")
        }
      }
    }

    @Trace
    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun listConnectionsForWorkspace(workspaceIdRequestBody: WorkspaceIdRequestBody): ConnectionReadList {
      addTagsToTrace(java.util.Map.of(MetricTags.WORKSPACE_ID, workspaceIdRequestBody.workspaceId.toString()))
      return listConnectionsForWorkspace(workspaceIdRequestBody, false)
    }

    @Trace
    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun listConnectionsForWorkspace(
      workspaceIdRequestBody: WorkspaceIdRequestBody,
      includeDeleted: Boolean,
    ): ConnectionReadList {
      val connectionReads: MutableList<ConnectionRead> = Lists.newArrayList()

      for (standardSync in connectionService.listWorkspaceStandardSyncs(workspaceIdRequestBody.workspaceId, includeDeleted)) {
        connectionReads.add(apiPojoConverters.internalToConnectionRead(standardSync))
      }

      return ConnectionReadList().connections(connectionReads)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun listAllConnectionsForWorkspace(workspaceIdRequestBody: WorkspaceIdRequestBody): ConnectionReadList =
      listConnectionsForWorkspace(workspaceIdRequestBody, true)

    @Throws(IOException::class)
    fun listConnectionsForSource(
      sourceId: UUID,
      includeDeleted: Boolean,
    ): ConnectionReadList {
      val connectionReads: MutableList<ConnectionRead> = Lists.newArrayList()
      for (standardSync in connectionService.listConnectionsBySource(sourceId, includeDeleted)) {
        connectionReads.add(apiPojoConverters.internalToConnectionRead(standardSync))
      }
      return ConnectionReadList().connections(connectionReads)
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun listConnections(): ConnectionReadList {
      val connectionReads: MutableList<ConnectionRead> = Lists.newArrayList()

      for (standardSync in connectionService.listStandardSyncs()) {
        if (standardSync.status == StandardSync.Status.DEPRECATED) {
          continue
        }
        connectionReads.add(apiPojoConverters.internalToConnectionRead(standardSync))
      }

      return ConnectionReadList().connections(connectionReads)
    }

    @Trace
    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun getConnection(connectionId: UUID): ConnectionRead = buildConnectionRead(connectionId)

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun getConnectionForJob(
      connectionId: UUID,
      jobId: Long,
    ): ConnectionRead = buildConnectionRead(connectionId, jobId)

    @Throws(JsonValidationException::class)
    fun getDiff(
      oldCatalog: AirbyteCatalog,
      newCatalog: AirbyteCatalog,
      configuredCatalog: ConfiguredAirbyteCatalog,
      connectionId: UUID?,
    ): CatalogDiff =
      CatalogDiff().transforms(
        getCatalogDiff(
          configuredCatalogToCatalog(catalogConverter.toProtocolKeepAllStreams(oldCatalog)),
          configuredCatalogToCatalog(catalogConverter.toProtocolKeepAllStreams(newCatalog)),
          configuredCatalog,
        ).stream()
          .map<StreamTransform> { obj: io.airbyte.commons.protocol.transformmodels.StreamTransform -> streamTransformToApi(obj) }
          .toList(),
      )

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, ConfigNotFoundException::class)
    fun getDiff(
      connectionRead: ConnectionRead,
      discoveredCatalog: AirbyteCatalog,
    ): CatalogDiff {
      val catalogWithSelectedFieldsAnnotated = connectionRead.syncCatalog
      val configuredCatalog = catalogConverter.toConfiguredInternal(catalogWithSelectedFieldsAnnotated)
      val rawCatalog = getConnectionAirbyteCatalog(connectionRead.connectionId)

      return getDiff(rawCatalog.orElse(catalogWithSelectedFieldsAnnotated), discoveredCatalog, configuredCatalog, connectionRead.connectionId)
    }

    /**
     * Returns the list of the streamDescriptor that have their config updated.
     *
     * @param oldCatalog the old catalog
     * @param newCatalog the new catalog
     * @return the list of StreamDescriptor that have their configuration changed
     */
    fun getConfigurationDiff(
      oldCatalog: AirbyteCatalog,
      newCatalog: AirbyteCatalog,
    ): Set<io.airbyte.api.model.generated.StreamDescriptor> {
      val oldStreams = catalogToPerStreamConfiguration(oldCatalog)
      val newStreams = catalogToPerStreamConfiguration(newCatalog)

      val streamWithDifferentConf: MutableSet<io.airbyte.api.model.generated.StreamDescriptor> = HashSet()

      newStreams.forEach(
        (
          BiConsumer { streamDescriptor: io.airbyte.api.model.generated.StreamDescriptor, airbyteStreamConfiguration: AirbyteStreamConfiguration ->
            val oldConfig =
              oldStreams[streamDescriptor]
            if (oldConfig != null && haveConfigChange(oldConfig, airbyteStreamConfiguration)) {
              streamWithDifferentConf.add(streamDescriptor)
            }
          }
        ),
      )

      return streamWithDifferentConf
    }

    private fun haveConfigChange(
      oldConfig: AirbyteStreamConfiguration,
      newConfig: AirbyteStreamConfiguration,
    ): Boolean {
      val oldCursors = oldConfig.cursorField
      val newCursors = newConfig.cursorField
      val hasCursorChanged = !(oldCursors == newCursors)

      val hasSyncModeChanged = oldConfig.syncMode != newConfig.syncMode

      val hasDestinationSyncModeChanged = oldConfig.destinationSyncMode != newConfig.destinationSyncMode

      val convertedOldPrimaryKey: Set<List<String>> = HashSet(oldConfig.primaryKey)
      val convertedNewPrimaryKey: Set<List<String>> = HashSet(newConfig.primaryKey)
      val hasPrimaryKeyChanged = !(convertedOldPrimaryKey == convertedNewPrimaryKey)

      return hasCursorChanged || hasSyncModeChanged || hasDestinationSyncModeChanged || hasPrimaryKeyChanged
    }

    private fun catalogToPerStreamConfiguration(
      catalog: AirbyteCatalog,
    ): Map<io.airbyte.api.model.generated.StreamDescriptor, AirbyteStreamConfiguration> =
      catalog.streams.stream().collect(
        Collectors.toMap<@Valid AirbyteStreamAndConfiguration?, io.airbyte.api.model.generated.StreamDescriptor, AirbyteStreamConfiguration>(
          { stream: AirbyteStreamAndConfiguration? ->
            io.airbyte.api.model.generated
              .StreamDescriptor()
              .name(stream!!.stream.name)
              .namespace(stream.stream.namespace)
          },
          { obj: AirbyteStreamAndConfiguration? -> obj!!.config },
        ),
      )

    @Trace
    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun getConnectionAirbyteCatalog(connectionId: UUID): Optional<AirbyteCatalog> {
      val connection = connectionService.getStandardSync(connectionId)
      if (connection.sourceCatalogId == null) {
        return Optional.empty()
      }
      val catalog = catalogService.getActorCatalogById(connection.sourceCatalogId)
      val sourceDefinition = sourceService.getSourceDefinitionFromSource(connection.sourceId)
      val sourceConnection = sourceService.getSourceConnection(connection.sourceId)
      val sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceConnection.workspaceId, connection.sourceId)
      val jsonCatalog =
        Jsons.`object`(catalog.catalog, io.airbyte.protocol.models.v0.AirbyteCatalog::class.java)
      val destination = destinationService.getDestinationDefinitionFromConnection(connectionId)
      // Note: we're using the workspace from the source to save an extra db request.
      val destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destination, sourceConnection.workspaceId)
      val supportedDestinationSyncModes =
        destinationVersion.spec.supportedDestinationSyncModes?.convertTo<DestinationSyncMode>() ?: emptyList()
      val convertedCatalog = Optional.of(catalogConverter.toApi(jsonCatalog, sourceVersion))
      if (convertedCatalog.isPresent) {
        convertedCatalog.get().streams.forEach(
          Consumer { streamAndConfiguration: AirbyteStreamAndConfiguration? ->
            catalogConverter.ensureCompatibleDestinationSyncMode(
              streamAndConfiguration!!,
              supportedDestinationSyncModes,
            )
          },
        )
      }
      return convertedCatalog
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun deleteConnection(connectionId: UUID) {
      connectionHelper.deleteConnection(connectionId)
      log.info { "Marked connectionId $connectionId as deleted in postgres" }
      eventRunner.forceDeleteConnection(connectionId)
      log.info { "Force-deleted connectionId $connectionId workflow" }
      streamRefreshesHandler.deleteRefreshesForConnection(connectionId)
      log.info { "Deleted connectionId $connectionId stream refreshes" }
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun buildConnectionRead(connectionId: UUID): ConnectionRead {
      val standardSync = connectionService.getStandardSync(connectionId)

      val maskedCatalog = mapperSecretHelper.maskMapperSecrets(standardSync.catalog)
      standardSync.catalog = maskedCatalog

      return apiPojoConverters.internalToConnectionRead(standardSync)
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    private fun buildConnectionRead(
      connectionId: UUID,
      jobId: Long,
    ): ConnectionRead {
      val standardSync = connectionService.getStandardSync(connectionId)
      val job = jobPersistence.getJob(jobId)
      val generations = streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(connectionId)
      val catalogWithGeneration =
        if (job.configType == ConfigType.SYNC) {
          Optional.of(
            catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
              standardSync.catalog,
              jobId,
              listOf(),
              generations,
            ),
          )
        } else if (job.configType == ConfigType.REFRESH) {
          Optional.of(
            catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
              standardSync.catalog,
              jobId,
              job.config.refresh.streamsToRefresh,
              generations,
            ),
          )
        } else if (job.configType == ConfigType.RESET_CONNECTION || job.configType == ConfigType.CLEAR) {
          Optional.of(
            catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformationForClear(
              standardSync.catalog,
              jobId,
              java.util.Set.copyOf(job.config.resetConnection.resetSourceConfiguration.streamsToReset),
              generations,
            ),
          )
        } else {
          Optional.empty()
        }

      catalogWithGeneration.ifPresent { catalog: ConfiguredAirbyteCatalog? ->
        standardSync.catalog =
          catalog
      }
      return apiPojoConverters.internalToConnectionRead(standardSync)
    }

    @Throws(IOException::class)
    fun listConnectionsForWorkspaces(listConnectionsForWorkspacesRequestBody: ListConnectionsForWorkspacesRequestBody): ConnectionReadList {
      val connectionReads: MutableList<ConnectionRead> = Lists.newArrayList()

      val workspaceIdToStandardSyncsMap =
        connectionService.listWorkspaceStandardSyncsLimitOffsetPaginated(
          listConnectionsForWorkspacesRequestBody.workspaceIds,
          listConnectionsForWorkspacesRequestBody.tagIds,
          listConnectionsForWorkspacesRequestBody.includeDeleted,
          pageSize(listConnectionsForWorkspacesRequestBody.pagination),
          rowOffset(listConnectionsForWorkspacesRequestBody.pagination),
        )

      for ((workspaceId, value) in workspaceIdToStandardSyncsMap) {
        for (standardSync in value) {
          val connectionRead = apiPojoConverters.internalToConnectionRead(standardSync)
          connectionRead.workspaceId = workspaceId
          connectionReads.add(connectionRead)
        }
      }
      return ConnectionReadList().connections(connectionReads)
    }

    @Throws(IOException::class)
    fun listConnectionsForActorDefinition(actorDefinitionRequestBody: ActorDefinitionRequestBody): ConnectionReadList {
      val connectionReads: MutableList<ConnectionRead> = ArrayList()

      val standardSyncs =
        connectionService.listConnectionsByActorDefinitionIdAndType(
          actorDefinitionRequestBody.actorDefinitionId,
          actorDefinitionRequestBody.actorType.toString(),
          false,
          true,
        )

      for (standardSync in standardSyncs) {
        val connectionRead = apiPojoConverters.internalToConnectionRead(standardSync)
        connectionReads.add(connectionRead)
      }
      return ConnectionReadList().connections(connectionReads)
    }

    fun mapFailureReason(data: FailureReason): io.airbyte.api.model.generated.FailureReason {
      val failureReason =
        io.airbyte.api.model.generated
          .FailureReason()
      failureReason.failureOrigin = data.failureOrigin?.convertTo<FailureOrigin>()
      failureReason.failureType = data.failureType?.convertTo<FailureType>()
      failureReason.externalMessage = data.externalMessage
      failureReason.internalMessage = data.internalMessage
      failureReason.stacktrace = data.stacktrace
      failureReason.retryable = data.retryable
      failureReason.timestamp = data.timestamp
      return failureReason
    }

    @Trace
    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    fun getConnectionStatuses(connectionStatusesRequestBody: ConnectionStatusesRequestBody): List<ConnectionStatusRead> {
      addTagsToTrace(java.util.Map.of(MetricTags.CONNECTION_IDS, connectionStatusesRequestBody.connectionIds.toString()))
      val connectionIds = connectionStatusesRequestBody.connectionIds
      val result: MutableList<ConnectionStatusRead> = ArrayList()
      for (connectionId in connectionIds) {
        val jobs =
          jobPersistence.listJobsLight(
            Job.REPLICATION_TYPES,
            connectionId.toString(),
            maxJobLookback,
          )
        val activeJob =
          jobs.stream().findFirst().filter { job: Job ->
            JobStatus.NON_TERMINAL_STATUSES.contains(
              job.status,
            )
          }
        val isRunning = activeJob.isPresent

        val lastSucceededOrFailedJob =
          jobs.stream().filter { job: Job -> JobStatus.TERMINAL_STATUSES.contains(job.status) && job.status != JobStatus.CANCELLED }.findFirst()
        val lastSyncStatus = lastSucceededOrFailedJob.map { j: Job -> j.status }
        val lastSyncJobStatus =
          lastSyncStatus.orElse(null)?.convertTo<io.airbyte.api.model.generated.JobStatus>()
        val lastJobWasCancelled = !jobs.isEmpty() && jobs.first().status == JobStatus.CANCELLED
        val lastJobWasResetOrClear =
          !jobs.isEmpty() &&
            (jobs.first().configType == ConfigType.RESET_CONNECTION || jobs.first().configType == ConfigType.CLEAR)

        val lastSuccessfulJob = jobs.stream().filter { job: Job -> job.status == JobStatus.SUCCEEDED }.findFirst()
        val lastSuccessTimestamp = lastSuccessfulJob.map { j: Job -> j.updatedAtInSecond }

        val connectionRead = buildConnectionRead(connectionId)
        val hasBreakingSchemaChange = connectionRead.breakingChange != null && connectionRead.breakingChange

        val connectionStatus =
          ConnectionStatusRead()
            .connectionId(connectionId)
            .activeJob(activeJob.map { obj: Job -> JobConverter.getJobRead(obj) }.orElse(null))
            .lastSuccessfulSync(lastSuccessTimestamp.orElse(null))
            .scheduleData(connectionRead.scheduleData)
        if (lastSucceededOrFailedJob.isPresent) {
          connectionStatus.lastSyncJobId(lastSucceededOrFailedJob.get().id)
          val lastAttempt = lastSucceededOrFailedJob.get().getLastAttempt()
          lastAttempt.ifPresent { attempt: Attempt -> connectionStatus.lastSyncAttemptNumber(attempt.getAttemptNumber()) }
        }
        val failureReason =
          lastSucceededOrFailedJob
            .flatMap { obj: Job -> obj.getLastFailedAttempt() }
            .flatMap { obj: Attempt -> obj.getFailureSummary() }
            .flatMap { s: AttemptFailureSummary -> s.failures.stream().findFirst() }
            .map { data: FailureReason -> this.mapFailureReason(data) }
        if (failureReason.isPresent && lastSucceededOrFailedJob.get().status == JobStatus.FAILED) {
          connectionStatus.failureReason = failureReason.get()
        }

        var hasConfigError = false
        if (lastSucceededOrFailedJob.isPresent && lastSucceededOrFailedJob.get().status == JobStatus.FAILED) {
          val failureReasons =
            lastSucceededOrFailedJob
              .flatMap { obj: Job -> obj.getLastFailedAttempt() }
              .flatMap { obj: Attempt -> obj.getFailureSummary() }
              .map { s: AttemptFailureSummary ->
                s.failures
                  .stream()
                  .map { data: FailureReason ->
                    this.mapFailureReason(
                      data,
                    )
                  }.collect(Collectors.toList())
              }

          if (failureReasons.isPresent && !failureReasons.get().isEmpty()) {
            connectionStatus.failureReason = failureReasons.get().first()

            hasConfigError =
              failureReasons
                .get()
                .stream()
                .anyMatch { reason: io.airbyte.api.model.generated.FailureReason -> reason.failureType == FailureType.CONFIG_ERROR }
          }
        }

        val latestSyncJob = jobPersistence.getLastSyncJob(connectionId).map { obj: Job -> JobConverter.getJobRead(obj) }
        latestSyncJob.ifPresent { job: JobRead ->
          connectionStatus.lastSyncJobCreatedAt = job.createdAt
        }

        if (isRunning) {
          connectionStatus.connectionSyncStatus = ConnectionSyncStatus.RUNNING
        } else if (hasBreakingSchemaChange || hasConfigError) {
          connectionStatus.connectionSyncStatus = ConnectionSyncStatus.FAILED
        } else if (connectionRead.status != ConnectionStatus.ACTIVE) {
          connectionStatus.connectionSyncStatus = ConnectionSyncStatus.PAUSED
        } else if (lastJobWasCancelled) {
          connectionStatus.connectionSyncStatus = ConnectionSyncStatus.INCOMPLETE
        } else if (lastSyncJobStatus == null || lastJobWasResetOrClear) {
          connectionStatus.connectionSyncStatus = ConnectionSyncStatus.PENDING
        } else if (lastSyncJobStatus == io.airbyte.api.model.generated.JobStatus.FAILED) {
          connectionStatus.connectionSyncStatus = ConnectionSyncStatus.INCOMPLETE
        } else {
          connectionStatus.connectionSyncStatus = ConnectionSyncStatus.SYNCED
        }

        result.add(connectionStatus)
      }

      return result
    }

    private fun convertConnectionType(eventTypes: List<ConnectionEventType>?): List<ConnectionEvent.Type>? {
      if (eventTypes == null) {
        return null
      }
      return eventTypes.stream().map { eventType: ConnectionEventType -> ConnectionEvent.Type.valueOf(eventType.name) }.collect(Collectors.toList())
    }

    private fun convertConnectionEvent(event: ConnectionTimelineEvent): io.airbyte.api.model.generated.ConnectionEvent {
      val connectionEvent =
        io.airbyte.api.model.generated
          .ConnectionEvent()
      connectionEvent.id(event.id)
      connectionEvent.eventType(ConnectionEventType.fromString(event.eventType))
      connectionEvent.createdAt(event.createdAt!!.toEpochSecond())
      connectionEvent.connectionId(event.connectionId)
      event.summary?.let { connectionEvent.summary(Jsons.deserialize(it)) }
      if (event.userId != null) {
        connectionEvent.user(connectionTimelineEventHelper.getUserReadInConnectionEvent(event.userId, event.connectionId))
      }
      return connectionEvent
    }

    private fun convertConnectionEventList(events: List<ConnectionTimelineEvent>): ConnectionEventList {
      val eventsRead =
        events.stream().map { event: ConnectionTimelineEvent -> this.convertConnectionEvent(event) }.collect(Collectors.toList())
      return ConnectionEventList().events(eventsRead)
    }

    fun listConnectionEvents(connectionEventsRequestBody: ConnectionEventsRequestBody): ConnectionEventList {
      // 1. set page size and offset
      val pageSize =
        if (connectionEventsRequestBody.pagination != null && connectionEventsRequestBody.pagination.pageSize != null) {
          connectionEventsRequestBody.pagination.pageSize
        } else {
          DEFAULT_PAGE_SIZE
        }
      val rowOffset =
        if (connectionEventsRequestBody.pagination != null && connectionEventsRequestBody.pagination.rowOffset != null) {
          connectionEventsRequestBody.pagination.rowOffset
        } else {
          DEFAULT_ROW_OFFSET
        }
      // 2. get list of events
      val events =
        connectionTimelineEventService.listEvents(
          connectionEventsRequestBody.connectionId,
          convertConnectionType(connectionEventsRequestBody.eventTypes),
          connectionEventsRequestBody.createdAtStart,
          connectionEventsRequestBody.createdAtEnd,
          pageSize,
          rowOffset,
        )
      return convertConnectionEventList(events)
    }

    fun getConnectionEvent(connectionEventIdRequestBody: ConnectionEventIdRequestBody): ConnectionEventWithDetails {
      val eventData = connectionTimelineEventService.getEvent(connectionEventIdRequestBody.connectionEventId)
      return hydrateConnectionEvent(eventData)
    }

    private fun hydrateConnectionEvent(event: ConnectionTimelineEvent): ConnectionEventWithDetails {
      val connectionEventWithDetails = ConnectionEventWithDetails()
      connectionEventWithDetails.id(event.id)
      connectionEventWithDetails.connectionId(event.connectionId)
      // enforce event type consistency
      connectionEventWithDetails.eventType(ConnectionEventType.fromString(event.eventType))
      event.summary?.let { connectionEventWithDetails.summary(Jsons.deserialize(it)) }
      // TODO(@keyi): implement details generation for different types of events.
      connectionEventWithDetails.details(null)
      connectionEventWithDetails.createdAt(event.createdAt!!.toEpochSecond())
      if (event.userId != null) {
        connectionEventWithDetails.user(connectionTimelineEventHelper.getUserReadInConnectionEvent(event.userId, event.connectionId))
      }
      return connectionEventWithDetails
    }

    /**
     * Backfill jobs to timeline events. Supported event types:
     *
     *
     * 1. SYNC_CANCELLED 2. SYNC_SUCCEEDED 3. SYNC_FAILED 4. SYNC_INCOMPLETE 5. REFRESH_SUCCEEDED 6.
     * REFRESH_FAILED 7. REFRESH_INCOMPLETE 8. REFRESH_CANCELLED 9. CLEAR_SUCCEEDED 10. CLEAR_FAILED 11.
     * CLEAR_INCOMPLETE 12. CLEAR_CANCELLED
     *
     * Notes:
     *
     *
     * 1. Manually started events (X_STARTED) will NOT be backfilled because we don't have that
     * information from Jobs table.
     *
     *
     *
     * 2. Manually cancelled events (X_CANCELLED) will be backfilled, but the associated user ID will be
     * missing as it is not trackable from Jobs table.
     *
     *
     *
     * 3. RESET_CONNECTION is just the old enum name of CLEAR.
     *
     */
    @Throws(IOException::class)
    fun backfillConnectionEvents(connectionEventsBackfillRequestBody: ConnectionEventsBackfillRequestBody) {
      val connectionId = connectionEventsBackfillRequestBody.connectionId
      val startTime = connectionEventsBackfillRequestBody.createdAtStart
      val endTime = connectionEventsBackfillRequestBody.createdAtEnd
      log.info { "Backfilled events from {} to {} for connection $startTime, endTime, connectionId" }
      // 1. list all jobs within a given time window
      val allJobsToMigrate =
        jobPersistence.listJobsForConvertingToEvents(
          java.util.Set.of(ConfigType.SYNC, ConfigType.REFRESH, ConfigType.RESET_CONNECTION),
          java.util.Set.of(JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.INCOMPLETE, JobStatus.CANCELLED),
          startTime,
          endTime,
        )
      log.info { "Verify listing jobs. $allJobsToMigrate.size jobs found." }
      // 2. For each job, log a timeline event
      allJobsToMigrate.forEach(
        Consumer { job: Job ->
          val jobRead = getJobWithAttemptsRead(job)
          // Construct a timeline event
          val event =
            if (job.status == JobStatus.FAILED || job.status == JobStatus.INCOMPLETE) {
              // We need to log a failed event with job stats and the first failure reason.
              FailedEvent(
                job.id,
                job.createdAtInSecond,
                job.updatedAtInSecond,
                jobRead.attempts
                  .stream()
                  .mapToLong { attempt: AttemptRead -> if (attempt.bytesSynced != null) attempt.bytesSynced else 0 }
                  .sum(),
                jobRead.attempts
                  .stream()
                  .mapToLong { attempt: AttemptRead -> if (attempt.recordsSynced != null) attempt.recordsSynced else 0 }
                  .sum(),
                null,
                job.getAttemptsCount(),
                job.configType.name,
                job.status.name,
                getStreamsAssociatedWithJob(job),
                null,
                job
                  .getLastAttempt()
                  .flatMap { obj: Attempt -> obj.getFailureSummary() }
                  .flatMap { summary: AttemptFailureSummary ->
                    summary.failures.stream().findFirst()
                  },
              )
            } else { // SUCCEEDED and CANCELLED
              // We need to log a timeline event with job stats.
              FinalStatusEvent(
                job.id,
                job.createdAtInSecond,
                job.updatedAtInSecond,
                jobRead.attempts
                  .stream()
                  .mapToLong { attempt: AttemptRead -> if (attempt.bytesSynced != null) attempt.bytesSynced else 0 }
                  .sum(),
                jobRead.attempts
                  .stream()
                  .mapToLong { attempt: AttemptRead -> if (attempt.recordsSynced != null) attempt.recordsSynced else 0 }
                  .sum(),
                null,
                job.getAttemptsCount(),
                job.configType.name,
                job.status.name,
                getStreamsAssociatedWithJob(job),
                null,
              )
            }
          // Save an event
          connectionTimelineEventService.writeEventWithTimestamp(
            UUID.fromString(job.scope),
            event,
            null,
            Instant.ofEpochSecond(job.updatedAtInSecond).atOffset(ZoneOffset.UTC),
          )
        },
      )
    }

    @Throws(IOException::class)
    fun listConnectionEventsMinimal(requestBody: ConnectionEventsListMinimalRequestBody): ConnectionEventListMinimal {
      if (requestBody.workspaceId == null) {
        return ConnectionEventListMinimal()
      }

      // Get all connection IDs for the workspace using the lightweight method
      val connectionIds = connectionService.listConnectionIdsForWorkspace(requestBody.workspaceId)

      val events =
        connectionTimelineEventService.listEventsMinimal(
          connectionIds,
          convertConnectionType(requestBody.eventTypes)!!,
          requestBody.createdAtStart,
          requestBody.createdAtEnd,
        )
      return ConnectionEventListMinimal().events(
        events.stream().map { timelineEvent: ConnectionTimelineEventMinimal -> this.minimalTimelineEventToApiResponse(timelineEvent) }.collect(
          Collectors.toList<@Valid ConnectionEventMinimal?>(),
        ),
      )
    }

    private fun minimalTimelineEventToApiResponse(timelineEvent: ConnectionTimelineEventMinimal): ConnectionEventMinimal =
      ConnectionEventMinimal()
        .connectionId(timelineEvent.connectionId)
        .connectionName(timelineEvent.connectionName)
        .createdAt(timelineEvent.createdAt)
        .eventType(ConnectionEventType.fromString(timelineEvent.eventType))
        .eventId(timelineEvent.id)

    /**
     * Returns data history for the given connection for requested number of jobs.
     *
     * @param connectionDataHistoryRequestBody connection Id and number of jobs
     * @return list of JobSyncResultRead
     */
    fun getConnectionDataHistory(connectionDataHistoryRequestBody: ConnectionDataHistoryRequestBody): List<JobSyncResultRead> {
      val jobs: List<Job>
      try {
        jobs =
          jobPersistence.listJobs(
            Job.SYNC_REPLICATION_TYPES,
            java.util.Set.of(JobStatus.SUCCEEDED, JobStatus.FAILED),
            connectionDataHistoryRequestBody.connectionId.toString(),
            connectionDataHistoryRequestBody.numberOfJobs,
          )
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
      val jobIdToJobRead = getJobIdToJobWithAttemptsReadMap(jobs, jobPersistence)

      val result: MutableList<JobSyncResultRead> = ArrayList()
      jobs.forEach(
        Consumer { job: Job ->
          val jobId = job.id
          val jobRead = jobIdToJobRead[jobId]!!.job
          val aggregatedStats = jobRead.aggregatedStats
          val jobResult =
            JobSyncResultRead()
              .jobId(jobId)
              .configType(jobRead.configType)
              .jobCreatedAt(jobRead.createdAt)
              .jobUpdatedAt(jobRead.updatedAt)
              .bytesEmitted(aggregatedStats.bytesEmitted)
              .bytesCommitted(aggregatedStats.bytesCommitted)
              .recordsEmitted(aggregatedStats.recordsEmitted)
              .recordsCommitted(aggregatedStats.recordsCommitted)
              .recordsRejected(aggregatedStats.recordsRejected)
          result.add(jobResult)
        },
      )

      // Sort the results by date
      return result
        .stream()
        .sorted(Comparator.comparing { obj: JobSyncResultRead -> obj.jobUpdatedAt })
        .collect(Collectors.toList())
    }

    /**
     * Returns records synced per stream per day for the given connection for the last 30 days in the
     * given timezone.
     *
     * @param connectionStreamHistoryRequestBody the connection id and timezone string
     * @return list of ConnectionStreamHistoryReadItems (timestamp, stream namespace, stream name,
     * records synced)
     */
    @Throws(IOException::class)
    fun getConnectionStreamHistory(connectionStreamHistoryRequestBody: ConnectionStreamHistoryRequestBody): List<ConnectionStreamHistoryReadItem> =
      getConnectionStreamHistoryInternal(connectionStreamHistoryRequestBody, Instant.now())

    fun getConnectionStreamHistoryInternal(
      connectionStreamHistoryRequestBody: ConnectionStreamHistoryRequestBody,
      instant: Instant,
    ): List<ConnectionStreamHistoryReadItem> {
      // Start time in designated timezone

      val endTimeInUserTimeZone = instant.atZone(ZoneId.of(connectionStreamHistoryRequestBody.timezone))
      val startTimeInUserTimeZone = endTimeInUserTimeZone.minusDays(30)
      // Convert start time to UTC (since that's what the database uses)
      val startTimeInUTC = startTimeInUserTimeZone.toInstant()

      val attempts =
        jobPersistence.listAttemptsForConnectionAfterTimestamp(
          connectionStreamHistoryRequestBody.connectionId,
          ConfigType.SYNC,
          startTimeInUTC,
        )

      val connectionStreamHistoryReadItemsByDate: NavigableMap<LocalDate, MutableMap<List<String>, Long>> = TreeMap()
      val userTimeZone = ZoneId.of(connectionStreamHistoryRequestBody.timezone)

      val startDate = startTimeInUserTimeZone.toLocalDate()
      val endDate = endTimeInUserTimeZone.toLocalDate()
      var date = startDate
      while (!date.isAfter(endDate)) {
        connectionStreamHistoryReadItemsByDate[date] = HashMap()
        date = date.plusDays(1)
      }

      for (attempt in attempts) {
        val endedAtOptional = attempt.attempt.getEndedAtInSecond()

        if (endedAtOptional.isPresent) {
          // Convert the endedAt timestamp from the database to the designated timezone
          val attemptEndedAt = Instant.ofEpochSecond(endedAtOptional.get())
          val attemptDateInUserTimeZone =
            attemptEndedAt
              .atZone(ZoneId.of(connectionStreamHistoryRequestBody.timezone))
              .toLocalDate()

          // Merge it with the records synced from the attempt
          val attemptOutput = attempt.attempt.getOutput()
          if (attemptOutput.isPresent) {
            val streamSyncStats =
              attemptOutput
                .get()
                .sync.standardSyncSummary.streamStats
            for (streamSyncStat in streamSyncStats) {
              val streamName = streamSyncStat.streamName
              val streamNamespace = streamSyncStat.streamNamespace
              val recordsCommitted = streamSyncStat.stats.recordsCommitted

              // Update the records loaded for the corresponding stream for that day
              val existingItem =
                connectionStreamHistoryReadItemsByDate[attemptDateInUserTimeZone]!!
              val key = java.util.List.of(streamNamespace, streamName)
              if (existingItem.containsKey(key)) {
                existingItem[key] = existingItem[key]!! + recordsCommitted
              } else {
                existingItem[key] = recordsCommitted
              }
            }
          }
        }
      }

      val result: MutableList<ConnectionStreamHistoryReadItem> = ArrayList()
      for ((date1, streamRecordsByStream) in connectionStreamHistoryReadItemsByDate) {
        streamRecordsByStream.entries
          .stream()
          .sorted(
            Comparator
              .comparing { e: Map.Entry<List<String>, Long> -> e.key[0] }
              .thenComparing { e: Map.Entry<List<String>, Long> -> e.key[1] },
          ).forEach { streamRecords: Map.Entry<List<String>, Long> ->
            val streamNamespaceAndName = streamRecords.key
            val recordsCommitted = streamRecords.value
            result.add(
              ConnectionStreamHistoryReadItem()
                .timestamp(Math.toIntExact(date1.atStartOfDay(userTimeZone).toEpochSecond()))
                .streamNamespace(streamNamespaceAndName[0])
                .streamName(streamNamespaceAndName[1])
                .recordsCommitted(recordsCommitted),
            )
          }
      }
      return result
    }

    @Throws(
      JsonValidationException::class,
      ConfigNotFoundException::class,
      IOException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun applySchemaChange(request: ConnectionAutoPropagateSchemaChange): ConnectionAutoPropagateResult =
      applySchemaChange(request.connectionId, request.workspaceId, request.catalogId, request.catalog, true)

    @Throws(
      JsonValidationException::class,
      ConfigNotFoundException::class,
      IOException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun applySchemaChange(
      connectionId: UUID,
      workspaceId: UUID,
      catalogId: UUID,
      catalog: AirbyteCatalog,
      autoApply: Boolean,
    ): ConnectionAutoPropagateResult {
      log.info { "Applying schema change for connection '$connectionId' only" }
      val connection = buildConnectionRead(connectionId)
      val catalogUsedToMakeConfiguredCatalog =
        getConnectionAirbyteCatalog(connectionId)
      val currentCatalog = connection.syncCatalog
      val diffToApply =
        getDiff(
          catalogUsedToMakeConfiguredCatalog.orElse(currentCatalog),
          catalog,
          catalogConverter.toConfiguredInternal(currentCatalog),
          connectionId,
        )
      val updateObject =
        ConnectionUpdate().connectionId(connection.connectionId)
      val destinationDefinitionId =
        destinationService.getDestinationDefinitionFromConnection(connection.connectionId).destinationDefinitionId
      val supportedDestinationSyncModes =
        connectorSpecHandler
          .getDestinationSpecification(
            DestinationDefinitionIdWithWorkspaceId()
              .destinationDefinitionId(destinationDefinitionId)
              .workspaceId(workspaceId),
          ).supportedDestinationSyncModes
      val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)
      val source = sourceService.getSourceConnection(connection.sourceId)
      val appliedDiff: CatalogDiff?
      if (applySchemaChangeHelper.shouldAutoPropagate(diffToApply, connection)) {
        // NOTE: appliedDiff is the part of the diff that were actually applied.
        appliedDiff =
          applySchemaChangeInternal(
            updateObject.connectionId,
            workspaceId,
            updateObject,
            currentCatalog,
            catalog,
            diffToApply.transforms,
            catalogId,
            connection.nonBreakingChangesPreference,
            supportedDestinationSyncModes,
          )
        updateConnection(updateObject, ConnectionAutoUpdatedReason.SCHEMA_CHANGE_AUTO_PROPAGATE.name, autoApply)
        log.info(
          "Propagating changes for connectionId: '{}', new catalogId '{}'",
          connection.connectionId,
          catalogId,
        )
        connectionTimelineEventHelper.logSchemaChangeAutoPropagationEventInConnectionTimeline(connectionId, appliedDiff)
        if (workspace.notificationSettings != null && workspace.email != null) {
          try {
            log.info { "Sending notification of schema auto propagation for connectionId: '$connection.connectionId'" }
            notificationHelper.notifySchemaPropagated(
              workspace.notificationSettings,
              appliedDiff,
              workspace,
              connection,
              source,
              workspace.email,
            )
          } catch (e: Exception) {
            log.info("Failed to send notification", e)
            addExceptionToTrace(e)
          }
        }
      } else {
        appliedDiff = null
        // Send notification to the user if schema change needs to be manually applied.
        if (workspace.notificationSettings != null && applySchemaChangeHelper.shouldManuallyApply(diffToApply, connection)) {
          try {
            log.info { "Sending notification of manually applying schema change for connectionId: '$connection.connectionId'" }
            notificationHelper.notifySchemaDiffToApply(
              workspace.notificationSettings,
              diffToApply,
              workspace,
              connection,
              source,
              workspace.email,
              connection.nonBreakingChangesPreference == NonBreakingChangesPreference.DISABLE,
            )
          } catch (e: Exception) {
            log.info("Failed to send notification", e)
            addExceptionToTrace(e)
          }
        }
      }
      return ConnectionAutoPropagateResult().propagatedDiff(appliedDiff)
    }

    private fun applySchemaChangeInternal(
      connectionId: UUID,
      workspaceId: UUID,
      updateObject: ConnectionUpdate,
      currentSyncCatalog: AirbyteCatalog,
      newCatalog: AirbyteCatalog,
      transformations: List<StreamTransform>,
      sourceCatalogId: UUID,
      nonBreakingChangesPreference: NonBreakingChangesPreference,
      supportedDestinationSyncModes: List<DestinationSyncMode?>,
    ): CatalogDiff {
      metricClient.count(
        OssMetricsRegistry.SCHEMA_CHANGE_AUTO_PROPAGATED,
        1L,
        MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
      )
      val propagateResult =
        applySchemaChangeHelper.getUpdatedSchema(
          currentSyncCatalog,
          newCatalog,
          transformations,
          nonBreakingChangesPreference,
          supportedDestinationSyncModes,
        )
      updateObject.syncCatalog = propagateResult.catalog
      updateObject.sourceCatalogId = sourceCatalogId
      trackSchemaChange(workspaceId, connectionId, propagateResult)
      return propagateResult.appliedDiff
    }

    private fun validateCatalogSize(
      catalog: AirbyteCatalog,
      workspaceId: UUID,
      operationName: String,
    ) {
      val validationContext = Workspace(workspaceId)
      val validationError = catalogValidator.fieldCount(catalog, validationContext)
      if (validationError != null) {
        metricClient.count(
          OssMetricsRegistry.CATALOG_SIZE_VALIDATION_ERROR,
          1L,
          MetricAttribute(MetricTags.CRUD_OPERATION, operationName),
          MetricAttribute(MetricTags.WORKSPACE_ID, workspaceId.toString()),
        )

        throw BadRequestException(validationError.message)
      }
    }

    fun trackSchemaChange(
      workspaceId: UUID,
      connectionId: UUID?,
      propagateResult: UpdateSchemaResult,
    ) {
      try {
        val changeEventTimeline = Instant.now().toString()
        for (streamTransform in propagateResult.appliedDiff.transforms) {
          val payload: MutableMap<String, Any?> = HashMap()
          payload["workspace_id"] = workspaceId
          payload["connection_id"] = connectionId
          payload["schema_change_event_date"] = changeEventTimeline
          payload["stream_change_type"] = streamTransform.transformType.toString()
          val streamDescriptor = streamTransform.streamDescriptor
          if (streamDescriptor.namespace != null) {
            payload["stream_namespace"] = streamDescriptor.namespace
          }
          payload["stream_name"] = streamDescriptor.name
          if (streamTransform.transformType == StreamTransform.TransformTypeEnum.UPDATE_STREAM) {
            payload["stream_field_changes"] =
              Jsons.serialize(streamTransform.updateStream)
          }
          trackingClient.track(workspaceId, ScopeType.WORKSPACE, "Schema Changes", payload)
        }
      } catch (e: Exception) {
        log.error(e) { "Error while sending tracking event for schema change" }
      }
    }

    @Trace
    fun getConnectionLastJobPerStream(req: ConnectionLastJobPerStreamRequestBody): List<ConnectionLastJobPerStreamReadItem> {
      addTagsToTrace(java.util.Map.of(MetricTags.CONNECTION_ID, req.connectionId.toString()))

      // determine the latest job ID with stats for each stream by calling the streamStatsService
      val streamToLastJobIdWithStats =
        streamStatusesService.getLastJobIdWithStatsByStream(req.connectionId)

      // retrieve the full job information for each of those latest jobs
      val jobs: List<Job>
      try {
        jobs = jobPersistence.listJobsLight(HashSet(streamToLastJobIdWithStats.values))
      } catch (e: IOException) {
        throw UnexpectedProblem("Failed to retrieve the latest job per stream", ProblemMessageData().message(e.message))
      }

      // hydrate those jobs with their aggregated stats
      val jobIdToJobRead = getJobIdToJobWithAttemptsReadMap(jobs, jobPersistence)

      // build a map of stream descriptor to job read
      val streamToJobRead =
        streamToLastJobIdWithStats.entries
          .stream()
          .collect(
            Collectors.toMap(
              { obj: Map.Entry<StreamDescriptor, Long> -> obj.key },
              { entry: Map.Entry<StreamDescriptor, Long> -> jobIdToJobRead[entry.value] },
            ),
          )

      // memoize the process of building a stat-by-stream map for each job
      val memo: MutableMap<Long, Map<StreamDescriptor, StreamStats>> = HashMap()

      // convert the hydrated jobs to the response format
      return streamToJobRead.entries
        .stream()
        .map { entry: Map.Entry<StreamDescriptor, JobWithAttemptsRead?> -> buildLastJobPerStreamReadItem(entry.key, entry.value!!.job, memo) }
        .collect(Collectors.toList())
    }

    /**
     * Does all secondary steps from a source discover for a connection. Currently, it calculates the
     * diff, conditionally disables and auto-propagates schema changes.
     */
    @Throws(
      JsonValidationException::class,
      ConfigNotFoundException::class,
      IOException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun postprocessDiscoveredCatalog(
      connectionId: UUID,
      discoveredCatalogId: UUID,
    ): PostprocessDiscoveredCatalogResult {
      val connection = connectionService.getStandardSync(connectionId)
      val mostRecentCatalog = catalogService.getMostRecentSourceActorCatalog(connection.sourceId)
      val mostRecentCatalogId = mostRecentCatalog.map { obj: ActorCatalogWithUpdatedAt -> obj.id }.orElse(discoveredCatalogId)
      val read = diffCatalogAndConditionallyDisable(connectionId, mostRecentCatalogId)

      val autoPropResult =
        applySchemaChange(connectionId, workspaceHelper.getWorkspaceForConnectionId(connectionId), mostRecentCatalogId, read.catalog, true)
      val diff = autoPropResult.propagatedDiff

      return PostprocessDiscoveredCatalogResult().appliedDiff(diff)
    }

    /**
     *
     * Disable the connection if: 1. there are schema breaking changes 2. there are non-breaking schema
     * changes but the connection is configured to disable for any schema changes
     *
     */
    @Throws(
      JsonValidationException::class,
      ConfigNotFoundException::class,
      IOException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun disableConnectionIfNeeded(
      connectionRead: ConnectionRead,
      containsBreakingChange: Boolean,
      diff: CatalogDiff,
    ): ConnectionRead {
      val connectionId = connectionRead.connectionId
      // Monitor the schema change detection
      if (containsBreakingChange) {
        metricClient.count(
          OssMetricsRegistry.BREAKING_SCHEMA_CHANGE_DETECTED,
          1L,
          MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
        )
      } else {
        metricClient.count(
          OssMetricsRegistry.NON_BREAKING_SCHEMA_CHANGE_DETECTED,
          1L,
          MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
        )
      }
      // Update connection
      // 1. update flag for breaking changes
      val patch =
        ConnectionUpdate()
          .breakingChange(containsBreakingChange)
          .connectionId(connectionId)
      // 2. disable connection and log a timeline event (connection_disabled) if needed
      var autoDisabledReason: ConnectionAutoDisabledReason? = null
      if (containsBreakingChange) {
        patch.status(ConnectionStatus.INACTIVE)
        autoDisabledReason = ConnectionAutoDisabledReason.SCHEMA_CHANGES_ARE_BREAKING
      } else if (connectionRead.nonBreakingChangesPreference == NonBreakingChangesPreference.DISABLE &&
        applySchemaChangeHelper.containsChanges(diff)
      ) {
        patch.status(ConnectionStatus.INACTIVE)
        autoDisabledReason = ConnectionAutoDisabledReason.DISABLE_CONNECTION_IF_ANY_SCHEMA_CHANGES
      }
      val updated = updateConnection(patch, autoDisabledReason?.name, true)
      return updated
    }

    /**
     * For a given discovered catalog and connection, calculate a catalog diff, determine if there are
     * breaking changes then disable the connection if necessary.
     */
    @Throws(
      JsonValidationException::class,
      ConfigNotFoundException::class,
      IOException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun diffCatalogAndConditionallyDisable(
      connectionId: UUID,
      discoveredCatalogId: UUID,
    ): SourceDiscoverSchemaRead {
      val connectionRead = getConnection(connectionId)
      val source = sourceService.getSourceConnection(connectionRead.sourceId)
      val sourceDef = sourceService.getStandardSourceDefinition(source.sourceDefinitionId)
      val sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.workspaceId, connectionRead.sourceId)

      val discoveredCatalog = retrieveDiscoveredCatalog(discoveredCatalogId, sourceVersion)

      val diff = getDiff(connectionRead, discoveredCatalog)
      val containsBreakingChange = applySchemaChangeHelper.containsBreakingChange(diff)
      val updatedConnection = disableConnectionIfNeeded(connectionRead, containsBreakingChange, diff)
      return SourceDiscoverSchemaRead()
        .breakingChange(containsBreakingChange)
        .catalogDiff(diff)
        .catalog(discoveredCatalog)
        .catalogId(discoveredCatalogId)
        .connectionStatus(updatedConnection.status)
    }

    fun getConnectionContext(connectionId: UUID): ConnectionContextRead {
      val domainModel = contextBuilder.fromConnectionId(connectionId)
      return domainModel.toServerApi()
    }

    @Throws(IOException::class, ConfigNotFoundException::class)
    private fun retrieveDiscoveredCatalog(
      catalogId: UUID,
      sourceVersion: ActorDefinitionVersion,
    ): AirbyteCatalog {
      val catalog = catalogService.getActorCatalogById(catalogId)
      val persistenceCatalog =
        Jsons.`object`(
          catalog.catalog,
          io.airbyte.protocol.models.v0.AirbyteCatalog::class.java,
        )
      return catalogConverter.toApi(persistenceCatalog, sourceVersion)
    }

    /**
     * Build a ConnectionLastJobPerStreamReadItem from a stream descriptor and a job read. This method
     * memoizes the stat-by-stream map for each job to avoid redundant computation in the case where
     * multiple streams are associated with the same job.
     */
    private fun buildLastJobPerStreamReadItem(
      streamDescriptor: StreamDescriptor,
      jobRead: JobRead,
      memo: MutableMap<Long, Map<StreamDescriptor, StreamStats>>,
    ): ConnectionLastJobPerStreamReadItem {
      // if this is the first time encountering the job, compute the stat-by-stream map for it
      memo.putIfAbsent(jobRead.id, buildStreamStatsMap(jobRead))

      // retrieve the stat for the stream of interest from the memo
      val statsForThisStream =
        Optional.ofNullable(
          memo[jobRead.id]!![streamDescriptor],
        )

      return ConnectionLastJobPerStreamReadItem()
        .streamName(streamDescriptor.name)
        .streamNamespace(streamDescriptor.namespace)
        .jobId(jobRead.id)
        .configType(jobRead.configType)
        .jobStatus(jobRead.status)
        .startedAt(jobRead.startedAt)
        .endedAt(jobRead.updatedAt) // assumes the job ended at the last updated time
        .bytesCommitted(statsForThisStream.map { obj: StreamStats -> obj.bytesCommitted }.orElse(null))
        .recordsCommitted(statsForThisStream.map { obj: StreamStats -> obj.recordsCommitted }.orElse(null))
    }

    /**
     * Build a map of stream descriptor to stream stats for a given job. This is only called at most
     * once per job, because the result is memoized.
     */
    private fun buildStreamStatsMap(jobRead: JobRead): Map<StreamDescriptor, StreamStats> {
      val map: MutableMap<StreamDescriptor, StreamStats> = HashMap()
      for (stat in jobRead.streamAggregatedStats) {
        val streamDescriptor =
          StreamDescriptor()
            .withName(stat.streamName)
            .withNamespace(stat.streamNamespace)
        map[streamDescriptor] = stat
      }
      return map
    }

    /**
     * Applies defaults to the config of a sync catalog based off catalog and actor definition versions.
     * Mainly here to apply includeFiles default logic — this can be deleted once we default to
     * includesFiles to true from the UI. Mutates!
     */
    @VisibleForTesting
    fun applyDefaultIncludeFiles(
      catalog: AirbyteCatalog,
      sourceVersion: ActorDefinitionVersion,
      destinationVersion: ActorDefinitionVersion,
    ): AirbyteCatalog {
      if (!sourceVersion.supportsFileTransfer) {
        return catalog
      }

      for (pair in catalog.streams) {
        val streamIsFileBased = pair.stream.isFileBased != null && pair.stream.isFileBased
        val includeFilesIsUnset = pair.config.includeFiles == null
        if (streamIsFileBased && includeFilesIsUnset) {
          val defaultValue = destinationVersion.supportsFileTransfer
          pair.config.includeFiles = defaultValue
        }
      }

      return catalog
    }

    companion object {
      private val log = KotlinLogging.logger {}
      const val DEFAULT_PAGE_SIZE: Int = 20
      const val DEFAULT_ROW_OFFSET: Int = 0

      private fun getFrequencyStringFromScheduleType(
        scheduleType: StandardSync.ScheduleType,
        scheduleData: ScheduleData,
      ): String =
        when (scheduleType) {
          StandardSync.ScheduleType.MANUAL -> {
            "manual"
          }

          StandardSync.ScheduleType.BASIC_SCHEDULE -> {
            TimeUnit.SECONDS
              .toMinutes(getIntervalInSecond(scheduleData.basicSchedule))
              .toString() + " min"
          }

          StandardSync.ScheduleType.CRON -> {
            // TODO(https://github.com/airbytehq/airbyte/issues/2170): consider something more detailed.
            "cron"
          }

          else -> {
            throw RuntimeException("Unexpected schedule type")
          }
        }
    }
  }
