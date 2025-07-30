/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Lists
import datadog.trace.api.Trace
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.CatalogConfigDiff
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationSnippetRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.OperationCreate
import io.airbyte.api.model.generated.OperationReadList
import io.airbyte.api.model.generated.OperationUpdate
import io.airbyte.api.model.generated.SchemaChange
import io.airbyte.api.model.generated.SelectedFieldInfo
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SourceSnippetRead
import io.airbyte.api.model.generated.StreamDescriptor
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.api.model.generated.WebBackendConnectionCreate
import io.airbyte.api.model.generated.WebBackendConnectionListItem
import io.airbyte.api.model.generated.WebBackendConnectionListRequestBody
import io.airbyte.api.model.generated.WebBackendConnectionRead
import io.airbyte.api.model.generated.WebBackendConnectionReadList
import io.airbyte.api.model.generated.WebBackendConnectionRequestBody
import io.airbyte.api.model.generated.WebBackendConnectionStatusCounts
import io.airbyte.api.model.generated.WebBackendConnectionUpdate
import io.airbyte.api.model.generated.WebBackendOperationCreateOrUpdate
import io.airbyte.api.model.generated.WebBackendWorkspaceState
import io.airbyte.api.model.generated.WebBackendWorkspaceStateResult
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.converters.ApiConverters.Companion.toInternal
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.helpers.CatalogConfigDiffHelper
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.config.ActorCatalogFetchEvent
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.Field
import io.airbyte.config.RefreshStream
import io.airbyte.config.Tag
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ConnectionWithJobInfo
import io.airbyte.data.services.shared.DEFAULT_PAGE_SIZE
import io.airbyte.data.services.shared.DestinationAndDefinition
import io.airbyte.data.services.shared.SourceAndDefinition
import io.airbyte.data.services.shared.StandardSyncQuery
import io.airbyte.data.services.shared.buildFilters
import io.airbyte.data.services.shared.parseSortKey
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Singleton
import jakarta.validation.Valid
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.function.Function
import java.util.stream.Collectors

/**
 * The web backend is an abstraction that allows the frontend to structure data in such a way that
 * it is easier for a react frontend to consume. It should NOT have direct access to the database.
 * It should operate exclusively by calling other endpoints that are exposed in the API.
 *
 * Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
class WebBackendConnectionsHandler(
  private val actorDefinitionVersionHandler: ActorDefinitionVersionHandler,
  private val connectionsHandler: ConnectionsHandler,
  private val stateHandler: StateHandler,
  private val sourceHandler: SourceHandler,
  private val destinationHandler: DestinationHandler,
  private val jobHistoryHandler: JobHistoryHandler,
  private val schedulerHandler: SchedulerHandler,
  private val operationsHandler: OperationsHandler,
  private val eventRunner: EventRunner,
  private val catalogService: CatalogService,
  private val connectionService: ConnectionService,
  // todo (cgardens) - this handler should NOT have access to the db. only access via handler.
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val fieldGenerator: FieldGenerator,
  private val destinationService: DestinationService,
  private val sourceService: SourceService,
  private val workspaceService: WorkspaceService,
  private val catalogConverter: CatalogConverter,
  private val applySchemaChangeHelper: ApplySchemaChangeHelper,
  private val apiPojoConverters: ApiPojoConverters,
  private val destinationCatalogGenerator: DestinationCatalogGenerator,
  private val connectionTimelineEventHelper: ConnectionTimelineEventHelper,
  private val catalogConfigDiffHelper: CatalogConfigDiffHelper,
) {
  @Throws(IOException::class)
  fun getWorkspaceState(webBackendWorkspaceState: WebBackendWorkspaceState): WebBackendWorkspaceStateResult {
    val workspaceId = webBackendWorkspaceState.workspaceId
    val connectionCount = workspaceService.countConnectionsForWorkspace(workspaceId)
    val destinationCount = workspaceService.countDestinationsForWorkspace(workspaceId)
    val sourceCount = workspaceService.countSourcesForWorkspace(workspaceId)

    return WebBackendWorkspaceStateResult()
      .hasConnections(connectionCount > 0)
      .hasDestinations(destinationCount > 0)
      .hasSources(sourceCount > 0)
  }

  @Throws(IOException::class)
  fun webBackendGetConnectionStatusCounts(workspaceIdRequestBody: WorkspaceIdRequestBody): WebBackendConnectionStatusCounts {
    val statusCounts = connectionService.getConnectionStatusCounts(workspaceIdRequestBody.workspaceId)
    return WebBackendConnectionStatusCounts()
      .running(statusCounts.running)
      .healthy(statusCounts.healthy)
      .failed(statusCounts.failed)
      .paused(statusCounts.paused)
      .notSynced(statusCounts.notSynced)
  }

  @Throws(IOException::class)
  fun getStateType(connectionIdRequestBody: ConnectionIdRequestBody): ConnectionStateType? =
    stateHandler.getState(connectionIdRequestBody).stateType.convertTo<ConnectionStateType>()

  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun webBackendListConnectionsForWorkspace(webBackendConnectionListRequestBody: WebBackendConnectionListRequestBody): WebBackendConnectionReadList {
    val filters = webBackendConnectionListRequestBody.filters
    val pageSize =
      if (webBackendConnectionListRequestBody.pageSize != null) {
        webBackendConnectionListRequestBody.pageSize
      } else {
        DEFAULT_PAGE_SIZE
      }

    val query =
      StandardSyncQuery(
        webBackendConnectionListRequestBody.workspaceId,
        webBackendConnectionListRequestBody.sourceId,
        webBackendConnectionListRequestBody.destinationId, // passing 'false' so that deleted connections are not included
        false,
      )

    // Parse sort key to extract field and direction
    val sortKeyInfo = parseSortKey(webBackendConnectionListRequestBody.sortKey)
    val internalSortKey = sortKeyInfo.sortKey
    val ascending = sortKeyInfo.ascending
    val connectionFilters = buildFilters(filters)

    val cursorPagination =
      connectionService.buildCursorPagination(
        webBackendConnectionListRequestBody.cursor,
        internalSortKey,
        connectionFilters,
        query,
        ascending,
        pageSize,
      )!!

    val numConnections =
      connectionService.countWorkspaceStandardSyncs(
        query,
        if (cursorPagination.cursor != null) cursorPagination.cursor?.filters else null,
      )

    val connectionsWithJobInfo =
      connectionService.listWorkspaceStandardSyncsCursorPaginated(query, cursorPagination!!)
    val sourceIds = connectionsWithJobInfo.stream().map { conn: ConnectionWithJobInfo -> conn.connection().sourceId }.toList()
    val destinationIds = connectionsWithJobInfo.stream().map { conn: ConnectionWithJobInfo -> conn.connection().destinationId }.toList()

    // Fetching all the related objects we need for the final output
    val sourceReadById = getSourceSnippetReadById(sourceIds)
    val destinationReadById = getDestinationSnippetReadById(destinationIds)
    val newestFetchEventsByActorId =
      catalogService.getMostRecentActorCatalogFetchEventForSources(sourceIds)

    val connectionItems: MutableList<WebBackendConnectionListItem> = Lists.newArrayList()

    for (connectionWithJobInfo in connectionsWithJobInfo) {
      connectionItems.add(
        buildWebBackendConnectionListItem(
          connectionWithJobInfo,
          sourceReadById,
          destinationReadById,
          Optional.ofNullable(newestFetchEventsByActorId[connectionWithJobInfo.connection().sourceId]),
        ),
      )
    }

    return WebBackendConnectionReadList().connections(connectionItems).pageSize(pageSize).numConnections(numConnections)
  }

  @Throws(IOException::class)
  private fun getSourceSnippetReadById(sourceIds: List<UUID>): Map<UUID, SourceSnippetRead> =
    sourceService
      .getSourceAndDefinitionsFromSourceIds(sourceIds)
      .stream()
      .map { sourceAndDefinition: SourceAndDefinition ->
        sourceHandler.toSourceSnippetRead(
          sourceAndDefinition.source,
          sourceAndDefinition.definition,
        )
      }.collect(
        Collectors.toMap(
          { obj: SourceSnippetRead -> obj.sourceId },
          Function.identity(),
        ),
      )

  @Throws(IOException::class)
  private fun getDestinationSnippetReadById(destinationIds: List<UUID>): Map<UUID, DestinationSnippetRead> =
    destinationService
      .getDestinationAndDefinitionsFromDestinationIds(destinationIds)
      .stream()
      .map { destinationAndDefinition: DestinationAndDefinition ->
        destinationHandler.toDestinationSnippetRead(
          destinationAndDefinition.destination,
          destinationAndDefinition.definition,
        )
      }.collect(
        Collectors.toMap(
          { obj: DestinationSnippetRead -> obj.destinationId },
          Function.identity(),
        ),
      )

  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  private fun buildWebBackendConnectionRead(
    connectionRead: ConnectionRead,
    currentSourceCatalogId: Optional<UUID>,
  ): WebBackendConnectionRead {
    val source = getSourceRead(connectionRead.sourceId)
    val destination = getDestinationRead(connectionRead.destinationId)
    val operations = getOperationReadList(connectionRead)
    val latestSyncJob = jobHistoryHandler.getLatestSyncJob(connectionRead.connectionId)
    val latestRunningSyncJob = jobHistoryHandler.getLatestRunningSyncJob(connectionRead.connectionId)

    val webBackendConnectionRead =
      getWebBackendConnectionRead(connectionRead, source, destination, operations)
        .catalogId(connectionRead.sourceCatalogId)

    webBackendConnectionRead.isSyncing = latestRunningSyncJob.isPresent

    latestSyncJob.ifPresent { job: JobRead ->
      webBackendConnectionRead.latestSyncJobCreatedAt = job.createdAt
      webBackendConnectionRead.latestSyncJobStatus = job.status
    }

    val mostRecentFetchEvent =
      catalogService.getMostRecentActorCatalogFetchEventForSource(connectionRead.sourceId)

    val schemaChange = getSchemaChange(connectionRead, currentSourceCatalogId, mostRecentFetchEvent)

    webBackendConnectionRead.schemaChange = schemaChange

    // find any scheduled or past breaking changes to the connectors
    val sourceActorDefinitionVersionRead =
      actorDefinitionVersionHandler
        .getActorDefinitionVersionForSourceId(SourceIdRequestBody().sourceId(source.sourceId))
    val destinationActorDefinitionVersionRead =
      actorDefinitionVersionHandler
        .getActorDefinitionVersionForDestinationId(DestinationIdRequestBody().destinationId(destination.destinationId))
    webBackendConnectionRead.sourceActorDefinitionVersion = sourceActorDefinitionVersionRead
    webBackendConnectionRead.destinationActorDefinitionVersion = destinationActorDefinitionVersionRead

    return webBackendConnectionRead
  }

  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  private fun buildWebBackendConnectionListItem(
    connectionWithJobInfo: ConnectionWithJobInfo,
    sourceReadById: Map<UUID, SourceSnippetRead>,
    destinationReadById: Map<UUID, DestinationSnippetRead>,
    latestFetchEvent: Optional<ActorCatalogFetchEvent>,
  ): WebBackendConnectionListItem {
    val source = sourceReadById[connectionWithJobInfo.connection().sourceId]
    val destination = destinationReadById[connectionWithJobInfo.connection().destinationId]
    val connectionRead = apiPojoConverters.internalToConnectionRead(connectionWithJobInfo.connection())
    val currentCatalogId = if (connectionRead == null) Optional.empty() else Optional.ofNullable(connectionRead.sourceCatalogId)

    val schemaChange = getSchemaChange(connectionRead, currentCatalogId, latestFetchEvent)

    // find any scheduled or past breaking changes to the connectors
    val sourceActorDefinitionVersionRead =
      actorDefinitionVersionHandler
        .getActorDefinitionVersionForSourceId(SourceIdRequestBody().sourceId(source!!.sourceId))
    val destinationActorDefinitionVersionRead =
      actorDefinitionVersionHandler
        .getActorDefinitionVersionForDestinationId(DestinationIdRequestBody().destinationId(destination!!.destinationId))

    val listItem =
      WebBackendConnectionListItem()
        .connectionId(connectionWithJobInfo.connection().connectionId)
        .status(apiPojoConverters.toApiStatus(connectionWithJobInfo.connection().status))
        .name(connectionWithJobInfo.connection().name)
        .scheduleType(apiPojoConverters.toApiConnectionScheduleType(connectionWithJobInfo.connection()))
        .scheduleData(apiPojoConverters.toApiConnectionScheduleData(connectionWithJobInfo.connection()))
        .source(source)
        .destination(destination)
        .isSyncing(
          connectionWithJobInfo
            .latestJobStatus()
            .map { status: JobStatus -> status == JobStatus.running }
            .orElse(false),
        ).schemaChange(schemaChange)
        .sourceActorDefinitionVersion(sourceActorDefinitionVersionRead)
        .destinationActorDefinitionVersion(destinationActorDefinitionVersionRead)
        .tags(
          connectionWithJobInfo
            .connection()
            .tags
            .stream()
            .map { tag: Tag -> this.buildTag(tag) }
            .toList(),
        )

    connectionWithJobInfo.latestJobCreatedAt().ifPresent { createdAt: OffsetDateTime ->
      listItem.latestSyncJobCreatedAt =
        createdAt.toEpochSecond()
    }
    connectionWithJobInfo.latestJobStatus().ifPresent { status: JobStatus ->
      listItem.latestSyncJobStatus =
        io.airbyte.api.model.generated.JobStatus
          .fromValue(status.literal)
    }

    return listItem
  }

  private fun buildTag(tag: Tag): io.airbyte.api.model.generated.Tag =
    io.airbyte.api.model.generated
      .Tag()
      .tagId(tag.tagId)
      .workspaceId(tag.workspaceId)
      .name(tag.name)
      .color(tag.color)

  @Trace
  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  private fun getSourceRead(sourceId: UUID): SourceRead {
    val sourceIdRequestBody = SourceIdRequestBody().sourceId(sourceId)
    return sourceHandler.getSource(sourceIdRequestBody)
  }

  @Trace
  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  private fun getDestinationRead(destinationId: UUID): DestinationRead {
    val destinationIdRequestBody = DestinationIdRequestBody().destinationId(destinationId)
    return destinationHandler.getDestination(destinationIdRequestBody)
  }

  @Trace
  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  private fun getOperationReadList(connectionRead: ConnectionRead): OperationReadList {
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionRead.connectionId)
    return operationsHandler.listOperationsForConnection(connectionIdRequestBody)
  }

  // todo (cgardens) - This logic is a headache to follow it stems from the internal data model not
  // tracking selected streams in any reasonable way. We should update that.
  @Trace
  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun webBackendGetConnection(webBackendConnectionRequestBody: WebBackendConnectionRequestBody): WebBackendConnectionRead {
    addTagsToTrace(java.util.Map.of(MetricTags.CONNECTION_ID, webBackendConnectionRequestBody.connectionId.toString()))
    val connectionIdRequestBody =
      ConnectionIdRequestBody()
        .connectionId(webBackendConnectionRequestBody.connectionId)

    val connection = connectionsHandler.getConnection(connectionIdRequestBody.connectionId)

        /*
         * This variable contains all configuration but will be missing streams that were not selected.
         */
    val configuredCatalog = connection.syncCatalog
        /*
         * This catalog represents the full catalog that was used to create the configured catalog. It will
         * have all streams that were present at the time. It will have default configuration options set.
         */
    val catalogUsedToMakeConfiguredCatalog =
      connectionsHandler
        .getConnectionAirbyteCatalog(webBackendConnectionRequestBody.connectionId)

        /*
         * This catalog represents the full catalog that exists now for the source. It will have default
         * configuration options set.
         */
    val refreshedCatalog =
      if (webBackendConnectionRequestBody.withRefreshedCatalog != null && webBackendConnectionRequestBody.withRefreshedCatalog) {
        getRefreshedSchema(connection.sourceId, connection.connectionId)
      } else {
        Optional.empty()
      }

    val diff: CatalogDiff?
    val syncCatalog: AirbyteCatalog
    val currentSourceCatalogId = Optional.ofNullable(connection.sourceCatalogId)
    if (refreshedCatalog.isPresent) {
      connection.sourceCatalogId(refreshedCatalog.get().catalogId)
            /*
             * constructs a full picture of all existing configured + all new / updated streams in the newest
             * catalog.
             *
             * Diffing the catalog used to make the configured catalog gives us the clearest diff between the
             * schema when the configured catalog was made and now. In the case where we do not have the
             * original catalog used to make the configured catalog, we make due, but using the configured
             * catalog itself. The drawback is that any stream that was not selected in the configured catalog
             * but was present at time of configuration will appear in the diff as an added stream which is
             * confusing. We need to figure out why source_catalog_id is not always populated in the db.
             */
      syncCatalog =
        updateSchemaWithRefreshedDiscoveredCatalog(
          configuredCatalog,
          catalogUsedToMakeConfiguredCatalog.orElse(configuredCatalog),
          refreshedCatalog.get().catalog,
        )

      diff = refreshedCatalog.get().catalogDiff
      connection.breakingChange = refreshedCatalog.get().breakingChange
      connection.status = refreshedCatalog.get().connectionStatus
    } else if (catalogUsedToMakeConfiguredCatalog.isPresent) {
      // reconstructs a full picture of the full schema at the time the catalog was configured.
      syncCatalog = updateSchemaWithOriginalDiscoveredCatalog(configuredCatalog, catalogUsedToMakeConfiguredCatalog.get())
      // diff not relevant if there was no refresh.
      diff = null
    } else {
      // fallback. over time this should be rarely used because source_catalog_id should always be set.
      syncCatalog = configuredCatalog
      // diff not relevant if there was no refresh.
      diff = null
    }

    connection.syncCatalog = syncCatalog
    return buildWebBackendConnectionRead(connection, currentSourceCatalogId).catalogDiff(diff)
  }

  private fun updateSchemaWithOriginalDiscoveredCatalog(
    configuredCatalog: AirbyteCatalog,
    originalDiscoveredCatalog: AirbyteCatalog,
  ): AirbyteCatalog {
    // We pass the original discovered catalog in as the "new" discovered catalog.
    return updateSchemaWithRefreshedDiscoveredCatalog(configuredCatalog, originalDiscoveredCatalog, originalDiscoveredCatalog)
  }

  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  private fun getRefreshedSchema(
    sourceId: UUID,
    connectionId: UUID,
  ): Optional<SourceDiscoverSchemaRead> {
    val discoverSchemaReadReq =
      SourceDiscoverSchemaRequestBody()
        .sourceId(sourceId)
        .disableCache(true)
        .connectionId(connectionId)
    val schemaRead = schedulerHandler.discoverSchemaForSourceFromSourceId(discoverSchemaReadReq)
    return Optional.ofNullable(schemaRead)
  }

  /**
   * Applies existing configurations to a newly discovered catalog. For example, if the users stream
   * is in the old and new catalog, any configuration that was previously set for users, we add to the
   * new catalog.
   *
   * @param originalConfigured fully configured, original catalog
   * @param originalDiscovered the original discovered catalog used to make the original configured
   * catalog
   * @param discovered newly discovered catalog, no configurations set
   * @return merged catalog, most up-to-date schema with most up-to-date configurations from old
   * catalog
   */
  @VisibleForTesting
  fun updateSchemaWithRefreshedDiscoveredCatalog(
    originalConfigured: AirbyteCatalog,
    originalDiscovered: AirbyteCatalog,
    discovered: AirbyteCatalog,
  ): AirbyteCatalog {
        /*
         * We can't directly use s.getStream() as the key, because it contains a bunch of other fields, so
         * we just define a quick-and-dirty record class.
         */
    val streamDescriptorToOriginalStream =
      originalConfigured.streams
        .stream()
        .collect(
          Collectors.toMap<@Valid AirbyteStreamAndConfiguration?, Stream, AirbyteStreamAndConfiguration?>(
            { s: AirbyteStreamAndConfiguration? ->
              Stream(
                s!!.stream.name,
                s.stream.namespace,
              )
            },
            { s: AirbyteStreamAndConfiguration? -> s },
          ),
        )
    val streamDescriptorToOriginalDiscoveredStream =
      originalDiscovered.streams
        .stream()
        .collect(
          Collectors.toMap<@Valid AirbyteStreamAndConfiguration?, Stream, AirbyteStreamAndConfiguration?>(
            { s: AirbyteStreamAndConfiguration? ->
              Stream(
                s!!.stream.name,
                s.stream.namespace,
              )
            },
            { s: AirbyteStreamAndConfiguration? -> s },
          ),
        )

    val streams: MutableList<AirbyteStreamAndConfiguration> = ArrayList()

    for (discoveredStream in discovered.streams) {
      val stream = discoveredStream.stream
      val originalConfiguredStream =
        streamDescriptorToOriginalStream[Stream(stream.name, stream.namespace)]
      val originalDiscoveredStream =
        streamDescriptorToOriginalDiscoveredStream[Stream(stream.name, stream.namespace)]
      val outputStreamConfig: AirbyteStreamConfiguration

      if (originalConfiguredStream != null) {
        val originalStreamConfig = originalConfiguredStream.config
        val discoveredStreamConfig = discoveredStream.config
        outputStreamConfig = AirbyteStreamConfiguration()

        if (stream.supportedSyncModes.contains(originalStreamConfig.syncMode)) {
          outputStreamConfig.syncMode = originalStreamConfig.syncMode
        } else {
          outputStreamConfig.syncMode = discoveredStreamConfig.syncMode
        }

        if (!originalStreamConfig.cursorField.isEmpty()) {
          outputStreamConfig.cursorField = originalStreamConfig.cursorField
        } else {
          outputStreamConfig.cursorField = discoveredStreamConfig.cursorField
        }

        outputStreamConfig.destinationSyncMode = originalStreamConfig.destinationSyncMode

        val hasSourceDefinedPK = stream.sourceDefinedPrimaryKey != null && !stream.sourceDefinedPrimaryKey.isEmpty()
        if (hasSourceDefinedPK) {
          outputStreamConfig.primaryKey = stream.sourceDefinedPrimaryKey
        } else if (!originalStreamConfig.primaryKey.isEmpty()) {
          outputStreamConfig.primaryKey = originalStreamConfig.primaryKey
        } else {
          outputStreamConfig.primaryKey = discoveredStreamConfig.primaryKey
        }

        outputStreamConfig.aliasName = originalStreamConfig.aliasName
        outputStreamConfig.selected = originalConfiguredStream.config.selected
        outputStreamConfig.suggested = originalConfiguredStream.config.suggested
        outputStreamConfig.includeFiles = originalConfiguredStream.config.includeFiles
        outputStreamConfig.fieldSelectionEnabled = originalStreamConfig.fieldSelectionEnabled
        outputStreamConfig.mappers = originalStreamConfig.mappers
        outputStreamConfig.destinationObjectName = originalStreamConfig.destinationObjectName

        // TODO(pedro): Handle other mappers that are no longer valid
        // Add hashed field configs that are still present in the schema
        if (originalStreamConfig.hashedFields != null && !originalStreamConfig.hashedFields.isEmpty()) {
          val discoveredFields =
            fieldGenerator
              .getFieldsFromSchema(stream.jsonSchema)
              .stream()
              .map(Field::name)
              .toList()
          for (hashedField in originalStreamConfig.hashedFields) {
            if (discoveredFields.contains(hashedField.fieldPath.first())) {
              outputStreamConfig.addHashedFieldsItem(hashedField)
            }
          }
        }

        if (outputStreamConfig.fieldSelectionEnabled) {
          // TODO(mfsiega-airbyte): support nested fields.
          // If field selection is enabled, populate the selected fields.
          val originallyDiscovered: MutableSet<String> = HashSet()
          val refreshDiscovered: MutableSet<String> = HashSet()
          // NOTE: by only taking the first element of the path, we're restricting to top-level fields.
          val originallySelected: Set<String> =
            HashSet(
              originalConfiguredStream.config.selectedFields
                .stream()
                .map { field: SelectedFieldInfo -> field.fieldPath[0] }
                .toList(),
            )
          originalDiscoveredStream!!
            .stream.jsonSchema
            .findPath("properties")
            .fieldNames()
            .forEachRemaining { e: String -> originallyDiscovered.add(e) }
          stream.jsonSchema
            .findPath("properties")
            .fieldNames()
            .forEachRemaining { e: String -> refreshDiscovered.add(e) }
          // We include a selected field if it:
          // (is in the newly discovered schema) AND (it was either originally selected OR not in the
          // originally discovered schema at all)
          // NOTE: this implies that the default behaviour for newly-discovered columns is to add them.
          for (discoveredField in refreshDiscovered) {
            if (originallySelected.contains(discoveredField) || !originallyDiscovered.contains(discoveredField)) {
              outputStreamConfig.addSelectedFieldsItem(SelectedFieldInfo().addFieldPathItem(discoveredField))
            }
          }
        } else {
          outputStreamConfig.selectedFields = listOf<@Valid SelectedFieldInfo?>()
        }
      } else {
        outputStreamConfig = discoveredStream.config
        outputStreamConfig.selected = false
      }
      val outputStream =
        AirbyteStreamAndConfiguration()
          .stream(Jsons.clone(stream))
          .config(outputStreamConfig)
      streams.add(outputStream)
    }
    return AirbyteCatalog().streams(streams)
  }

  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun webBackendCreateConnection(webBackendConnectionCreate: WebBackendConnectionCreate): WebBackendConnectionRead {
    val operationIds = createOperations(webBackendConnectionCreate)

    val connectionCreate = toConnectionCreate(webBackendConnectionCreate, operationIds)
    val currentSourceCatalogId = Optional.ofNullable(connectionCreate.sourceCatalogId)
    return buildWebBackendConnectionRead(connectionsHandler.createConnection(connectionCreate), currentSourceCatalogId)
  }

  /**
   * Given a WebBackendConnectionUpdate, patch the connection by applying any non-null properties from
   * the patch to the connection.
   *
   * As a convenience to the front-end, this endpoint also creates new operations present in the
   * request, and bundles those newly-created operationIds into the connection update.
   */
  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  fun webBackendUpdateConnection(webBackendConnectionPatch: WebBackendConnectionUpdate): WebBackendConnectionRead {
    val connectionId = webBackendConnectionPatch.connectionId
    val originalConnectionRead = connectionsHandler.getConnection(connectionId)
    var breakingChange = originalConnectionRead.breakingChange != null && originalConnectionRead.breakingChange
    val source = getSourceRead(originalConnectionRead.sourceId)

    // If there have been changes to the sync catalog, check whether these changes result in or fix a
    // broken connection
    var catalogConfigDiff: CatalogConfigDiff? = null // Config diff
    var schemaDiff: CatalogDiff? = null // Schema diff
    if (webBackendConnectionPatch.syncCatalog != null) {
      // Get the most recent actor catalog fetched for this connection's source and the newly updated sync
      // catalog
      val mostRecentActorCatalog =
        catalogService.getMostRecentActorCatalogForSource(originalConnectionRead.sourceId)
      val newAirbyteCatalog = webBackendConnectionPatch.syncCatalog
      // Get the diff between these two catalogs to check for breaking changes
      if (mostRecentActorCatalog.isPresent) {
        val mostRecentAirbyteCatalog =
          Jsons.`object`(mostRecentActorCatalog.get().catalog, io.airbyte.protocol.models.v0.AirbyteCatalog::class.java)
        val sourceDefinition =
          sourceService.getSourceDefinitionFromSource(originalConnectionRead.sourceId)
        val sourceVersion =
          actorDefinitionVersionHelper.getSourceVersion(
            sourceDefinition,
            source.workspaceId,
            source.sourceId,
          )
        val catalogDiff =
          connectionsHandler.getDiff(
            newAirbyteCatalog,
            catalogConverter.toApi(mostRecentAirbyteCatalog, sourceVersion),
            catalogConverter.toConfiguredInternal(newAirbyteCatalog),
            connectionId,
          )
        breakingChange = applySchemaChangeHelper.containsBreakingChange(catalogDiff)

        val mostRecentSourceCatalog = catalogConverter.toApi(mostRecentAirbyteCatalog, sourceVersion)
        // Get the schema diff between the current raw catalog and the most recent source catalog
        schemaDiff = connectionsHandler.getDiff(originalConnectionRead, mostRecentSourceCatalog)
        // Get the catalog config diff between the current sync config and new sync config
        catalogConfigDiff =
          catalogConfigDiffHelper.getCatalogConfigDiff(
            mostRecentSourceCatalog,
            originalConnectionRead.syncCatalog,
            newAirbyteCatalog,
          )
      }
    }

    // before doing any updates, fetch the existing catalog so that it can be diffed
    // with the final catalog to determine which streams might need to be reset.
    val oldConfiguredCatalog =
      connectionService.getConfiguredCatalogForConnection(connectionId)

    val newAndExistingOperationIds = createOrUpdateOperations(originalConnectionRead, webBackendConnectionPatch)

    // pass in operationIds because the patch object doesn't include operationIds that were just created
    // above.
    val connectionPatch = toConnectionPatch(webBackendConnectionPatch, newAndExistingOperationIds, breakingChange)

    // persist the update and set the connectionRead to the updated form.
    val updatedConnectionRead = connectionsHandler.updateConnection(connectionPatch, null, false)
    // log a schema_config_change event in connection timeline.
    val airbyteCatalogDiff = catalogConfigDiffHelper.getAirbyteCatalogDiff(schemaDiff, catalogConfigDiff)
    if (airbyteCatalogDiff != null) {
      connectionTimelineEventHelper.logSchemaConfigChangeEventInConnectionTimeline(connectionId, airbyteCatalogDiff)
    }

    // detect if any streams need to be reset based on the patch and initial catalog, if so, reset them
    resetStreamsIfNeeded(webBackendConnectionPatch, oldConfiguredCatalog, updatedConnectionRead, originalConnectionRead)
        /*
         * This catalog represents the full catalog that was used to create the configured catalog. It will
         * have all streams that were present at the time. It will have no configuration set.
         */
    val catalogUsedToMakeConfiguredCatalog =
      connectionsHandler
        .getConnectionAirbyteCatalog(connectionId)
    if (catalogUsedToMakeConfiguredCatalog.isPresent) {
      // Update the Catalog returned to include all streams, including disabled ones
      val syncCatalog =
        updateSchemaWithRefreshedDiscoveredCatalog(
          updatedConnectionRead.syncCatalog,
          catalogUsedToMakeConfiguredCatalog.get(),
          catalogUsedToMakeConfiguredCatalog.get(),
        )
      updatedConnectionRead.syncCatalog = syncCatalog
    }

    val currentSourceCatalogId = Optional.ofNullable(updatedConnectionRead.sourceCatalogId)
    return buildWebBackendConnectionRead(updatedConnectionRead, currentSourceCatalogId)
  }

  /**
   * Given a fully updated connection, check for a diff between the old catalog and the updated
   * catalog to see if any streams need to be reset.
   */
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.data.ConfigNotFoundException::class,
  )
  private fun resetStreamsIfNeeded(
    webBackendConnectionPatch: WebBackendConnectionUpdate,
    oldConfiguredCatalog: ConfiguredAirbyteCatalog,
    updatedConnectionRead: ConnectionRead,
    oldConnectionRead: ConnectionRead,
  ) {
    val connectionId = webBackendConnectionPatch.connectionId
    val skipReset = if (webBackendConnectionPatch.skipReset != null) webBackendConnectionPatch.skipReset else false
    if (skipReset) {
      return
    }

    // Use destination catalogs when computing diffs so mappings are taken into account
    val updatedConfiguredCatalog = catalogConverter.toConfiguredInternal(updatedConnectionRead.syncCatalog)
    val updatedDestinationCatalog =
      destinationCatalogGenerator
        .generateDestinationCatalog(updatedConfiguredCatalog)
        .catalog
    val oldDestinationCatalog =
      destinationCatalogGenerator
        .generateDestinationCatalog(oldConfiguredCatalog)
        .catalog

    val apiExistingCatalog =
      catalogConverter.toApi(
        oldDestinationCatalog,
        catalogConverter.getFieldSelectionData(oldConnectionRead.syncCatalog),
      )
    val apiUpdatedCatalog =
      catalogConverter.toApi(
        updatedDestinationCatalog,
        catalogConverter.getFieldSelectionData(updatedConnectionRead.syncCatalog),
      )

    val catalogDiff =
      connectionsHandler.getDiff(apiExistingCatalog, apiUpdatedCatalog, updatedDestinationCatalog, connectionId)
    val apiStreamsToReset = getStreamsToReset(catalogDiff)
    val changedConfigStreamDescriptors =
      connectionsHandler.getConfigurationDiff(apiExistingCatalog, apiUpdatedCatalog)
    val allStreamToReset: MutableSet<StreamDescriptor> = HashSet()
    allStreamToReset.addAll(apiStreamsToReset)
    allStreamToReset.addAll(changedConfigStreamDescriptors)
    val streamsToReset =
      allStreamToReset
        .stream()
        .map { obj: StreamDescriptor -> obj.toInternal() }
        .toList()

    if (!streamsToReset.isEmpty()) {
      val destinationVersion =
        actorDefinitionVersionHandler
          .getActorDefinitionVersionForDestinationId(DestinationIdRequestBody().destinationId(oldConnectionRead.destinationId))
      if (destinationVersion.supportsRefreshes) {
        eventRunner.refreshConnectionAsync(
          connectionId,
          streamsToReset,
          RefreshStream.RefreshType.MERGE,
        )
      } else {
        eventRunner.resetConnectionAsync(
          connectionId,
          streamsToReset,
        )
      }
    }
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  private fun createOperations(webBackendConnectionCreate: WebBackendConnectionCreate): List<UUID> {
    if (webBackendConnectionCreate.operations == null) {
      return emptyList()
    }
    val operationIds: MutableList<UUID> = ArrayList()
    for (operationCreate in webBackendConnectionCreate.operations) {
      operationIds.add(operationsHandler.createOperation(operationCreate).operationId)
    }
    return operationIds
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, io.airbyte.data.ConfigNotFoundException::class)
  private fun createOrUpdateOperations(
    connectionRead: ConnectionRead,
    webBackendConnectionPatch: WebBackendConnectionUpdate,
  ): List<UUID>? {
    // this is a patch-style update, so don't make any changes if the request doesn't include operations

    if (webBackendConnectionPatch.operations == null) {
      return null
    }

    // wrap operationIds in a new ArrayList so that it is modifiable below, when calling .removeAll
    val originalOperationIds: MutableList<UUID> =
      if (connectionRead.operationIds == null) ArrayList() else ArrayList(connectionRead.operationIds)

    val updatedOperations = webBackendConnectionPatch.operations
    val finalOperationIds: MutableList<UUID> = ArrayList()

    for (operationCreateOrUpdate in updatedOperations) {
      if (operationCreateOrUpdate.operationId == null || !originalOperationIds.contains(operationCreateOrUpdate.operationId)) {
        val operationCreate = toOperationCreate(operationCreateOrUpdate)
        finalOperationIds.add(operationsHandler.createOperation(operationCreate).operationId)
      } else {
        val operationUpdate = toOperationUpdate(operationCreateOrUpdate)
        finalOperationIds.add(operationsHandler.updateOperation(operationUpdate).operationId)
      }
    }

    // remove operationIds that weren't included in the update
    originalOperationIds.removeAll(finalOperationIds)
    operationsHandler.deleteOperationsForConnection(connectionRead.connectionId, originalOperationIds)
    return finalOperationIds
  }

  /**
   * Equivalent to {@see io.airbyte.integrations.base.AirbyteStreamNameNamespacePair}. Intentionally
   * not using that class because it doesn't make sense for airbyte-server to depend on
   * base-java-integration.
   */
  @JvmRecord
  private data class Stream(
    val name: String,
    val namespace: String?,
  )

  companion object {
        /*
         * A breakingChange boolean is stored on the connectionRead object and corresponds to the boolean
         * breakingChange field on the connection table. If there is not a breaking change, we still have to
         * check whether there is a non-breaking schema change by fetching the most recent
         * ActorCatalogFetchEvent. A new ActorCatalogFetchEvent is stored each time there is a source schema
         * refresh, so if the most recent ActorCatalogFetchEvent has a different actor catalog than the
         * existing actor catalog, there is a schema change.
         */
    @VisibleForTesting
    @JvmStatic
    fun getSchemaChange(
      connectionRead: ConnectionRead?,
      currentSourceCatalogId: Optional<UUID>,
      mostRecentFetchEvent: Optional<ActorCatalogFetchEvent>,
    ): SchemaChange {
      if (connectionRead == null || currentSourceCatalogId.isEmpty) {
        return SchemaChange.NO_CHANGE
      }

      if (connectionRead.breakingChange != null && connectionRead.breakingChange) {
        return SchemaChange.BREAKING
      }

      if (mostRecentFetchEvent.isPresent &&
        mostRecentFetchEvent.map { obj: ActorCatalogFetchEvent -> obj.actorCatalogId } != currentSourceCatalogId
      ) {
        return SchemaChange.NON_BREAKING
      }

      return SchemaChange.NO_CHANGE
    }

    private fun getWebBackendConnectionRead(
      connectionRead: ConnectionRead,
      source: SourceRead,
      destination: DestinationRead,
      operations: OperationReadList,
    ): WebBackendConnectionRead =
      WebBackendConnectionRead()
        .connectionId(connectionRead.connectionId)
        .sourceId(connectionRead.sourceId)
        .destinationId(connectionRead.destinationId)
        .operationIds(connectionRead.operationIds)
        .name(connectionRead.name)
        .namespaceDefinition(connectionRead.namespaceDefinition)
        .namespaceFormat(connectionRead.namespaceFormat)
        .prefix(connectionRead.prefix)
        .syncCatalog(connectionRead.syncCatalog)
        .destinationCatalogId(connectionRead.destinationCatalogId)
        .status(connectionRead.status)
        .schedule(connectionRead.schedule)
        .scheduleType(connectionRead.scheduleType)
        .scheduleData(connectionRead.scheduleData)
        .source(source)
        .destination(destination)
        .operations(operations.operations)
        .dataplaneGroupId(connectionRead.dataplaneGroupId)
        .resourceRequirements(connectionRead.resourceRequirements)
        .notifySchemaChanges(connectionRead.notifySchemaChanges)
        .notifySchemaChangesByEmail(connectionRead.notifySchemaChangesByEmail)
        .createdAt(connectionRead.createdAt)
        .nonBreakingChangesPreference(connectionRead.nonBreakingChangesPreference)
        .backfillPreference(connectionRead.backfillPreference)
        .tags(connectionRead.tags)

    @VisibleForTesting
    protected fun toOperationCreate(operationCreateOrUpdate: WebBackendOperationCreateOrUpdate): OperationCreate {
      val operationCreate = OperationCreate()

      operationCreate.name(operationCreateOrUpdate.name)
      operationCreate.workspaceId(operationCreateOrUpdate.workspaceId)
      operationCreate.operatorConfiguration(operationCreateOrUpdate.operatorConfiguration)

      return operationCreate
    }

    @JvmStatic
    @VisibleForTesting
    fun toOperationUpdate(operationCreateOrUpdate: WebBackendOperationCreateOrUpdate): OperationUpdate {
      val operationUpdate = OperationUpdate()

      operationUpdate.operationId(operationCreateOrUpdate.operationId)
      operationUpdate.name(operationCreateOrUpdate.name)
      operationUpdate.operatorConfiguration(operationCreateOrUpdate.operatorConfiguration)

      return operationUpdate
    }

    @VisibleForTesting
    @JvmStatic
    fun toConnectionCreate(
      webBackendConnectionCreate: WebBackendConnectionCreate,
      operationIds: List<UUID?>?,
    ): ConnectionCreate {
      val connectionCreate = ConnectionCreate()

      connectionCreate.name(webBackendConnectionCreate.name)
      connectionCreate.namespaceDefinition(webBackendConnectionCreate.namespaceDefinition)
      connectionCreate.namespaceFormat(webBackendConnectionCreate.namespaceFormat)
      connectionCreate.prefix(webBackendConnectionCreate.prefix)
      connectionCreate.sourceId(webBackendConnectionCreate.sourceId)
      connectionCreate.destinationId(webBackendConnectionCreate.destinationId)
      connectionCreate.operationIds(operationIds)
      connectionCreate.syncCatalog(webBackendConnectionCreate.syncCatalog)
      connectionCreate.schedule(webBackendConnectionCreate.schedule)
      connectionCreate.scheduleType(webBackendConnectionCreate.scheduleType)
      connectionCreate.scheduleData(webBackendConnectionCreate.scheduleData)
      connectionCreate.status(webBackendConnectionCreate.status)
      connectionCreate.resourceRequirements(webBackendConnectionCreate.resourceRequirements)
      connectionCreate.sourceCatalogId(webBackendConnectionCreate.sourceCatalogId)
      connectionCreate.destinationCatalogId(webBackendConnectionCreate.destinationCatalogId)
      connectionCreate.dataplaneGroupId(webBackendConnectionCreate.dataplaneGroupId)
      connectionCreate.notifySchemaChanges(webBackendConnectionCreate.notifySchemaChanges)
      connectionCreate.nonBreakingChangesPreference(webBackendConnectionCreate.nonBreakingChangesPreference)
      connectionCreate.backfillPreference(webBackendConnectionCreate.backfillPreference)
      connectionCreate.tags(webBackendConnectionCreate.tags)

      return connectionCreate
    }

    /**
     * Take in a WebBackendConnectionUpdate and convert it into a ConnectionUpdate. OperationIds are
     * handled as a special case because the WebBackendConnectionUpdate handler allows for on-the-fly
     * creation of new operations. So, the brand-new IDs are passed in because they aren't present in
     * the WebBackendConnectionUpdate itself.
     *
     * The return value is used as a patch -- a field set to null means that it should not be modified.
     */
    @VisibleForTesting
    @JvmStatic
    fun toConnectionPatch(
      webBackendConnectionPatch: WebBackendConnectionUpdate,
      finalOperationIds: List<UUID?>?,
      breakingChange: Boolean,
    ): ConnectionUpdate {
      val connectionPatch = ConnectionUpdate()

      connectionPatch.connectionId(webBackendConnectionPatch.connectionId)
      connectionPatch.namespaceDefinition(webBackendConnectionPatch.namespaceDefinition)
      connectionPatch.namespaceFormat(webBackendConnectionPatch.namespaceFormat)
      connectionPatch.prefix(webBackendConnectionPatch.prefix)
      connectionPatch.name(webBackendConnectionPatch.name)
      connectionPatch.syncCatalog(webBackendConnectionPatch.syncCatalog)
      connectionPatch.schedule(webBackendConnectionPatch.schedule)
      connectionPatch.scheduleType(webBackendConnectionPatch.scheduleType)
      connectionPatch.scheduleData(webBackendConnectionPatch.scheduleData)
      connectionPatch.status(webBackendConnectionPatch.status)
      connectionPatch.resourceRequirements(webBackendConnectionPatch.resourceRequirements)
      connectionPatch.sourceCatalogId(webBackendConnectionPatch.sourceCatalogId)
      connectionPatch.destinationCatalogId(webBackendConnectionPatch.destinationCatalogId)
      connectionPatch.dataplaneGroupId(webBackendConnectionPatch.dataplaneGroupId)
      connectionPatch.notifySchemaChanges(webBackendConnectionPatch.notifySchemaChanges)
      connectionPatch.notifySchemaChangesByEmail(webBackendConnectionPatch.notifySchemaChangesByEmail)
      connectionPatch.nonBreakingChangesPreference(webBackendConnectionPatch.nonBreakingChangesPreference)
      connectionPatch.backfillPreference(webBackendConnectionPatch.backfillPreference)
      connectionPatch.breakingChange(breakingChange)
      connectionPatch.tags(webBackendConnectionPatch.tags)

      connectionPatch.operationIds(finalOperationIds)

      return connectionPatch
    }

    @JvmStatic
    @VisibleForTesting
    fun getStreamsToReset(catalogDiff: CatalogDiff): List<StreamDescriptor> =
      catalogDiff.transforms
        .stream()
        .map { obj: StreamTransform -> obj.streamDescriptor }
        .toList()
  }
}
