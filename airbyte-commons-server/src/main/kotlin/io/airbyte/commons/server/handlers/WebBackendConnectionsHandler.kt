/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.ActorDefinitionVersionRead
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.CatalogConfigDiff
import io.airbyte.api.server.generated.models.CatalogDiff
import io.airbyte.api.server.generated.models.ConnectionCreate
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionStateType
import io.airbyte.api.server.generated.models.ConnectionUpdate
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.DestinationRead
import io.airbyte.api.server.generated.models.DestinationSnippetRead
import io.airbyte.api.server.generated.models.JobRead
import io.airbyte.api.server.generated.models.OperationCreate
import io.airbyte.api.server.generated.models.OperationReadList
import io.airbyte.api.server.generated.models.OperationUpdate
import io.airbyte.api.server.generated.models.SchemaChange
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRead
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRequestBody
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.SourceSnippetRead
import io.airbyte.api.server.generated.models.StreamDescriptor
import io.airbyte.api.server.generated.models.StreamTransform
import io.airbyte.api.server.generated.models.WebBackendConnectionCreate
import io.airbyte.api.server.generated.models.WebBackendConnectionListItem
import io.airbyte.api.server.generated.models.WebBackendConnectionListRequestBody
import io.airbyte.api.server.generated.models.WebBackendConnectionRead
import io.airbyte.api.server.generated.models.WebBackendConnectionReadList
import io.airbyte.api.server.generated.models.WebBackendConnectionRequestBody
import io.airbyte.api.server.generated.models.WebBackendConnectionStatusCounts
import io.airbyte.api.server.generated.models.WebBackendConnectionUpdate
import io.airbyte.api.server.generated.models.WebBackendOperationCreateOrUpdate
import io.airbyte.api.server.generated.models.WebBackendWorkspaceState
import io.airbyte.api.server.generated.models.WebBackendWorkspaceStateResult
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.converters.ApiConverters.Companion.toInternal
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.handlers.helpers.CatalogMergeHelper
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.helpers.CatalogConfigDiffHelper
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.config.ActorCatalogFetchEvent
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.RefreshStream
import io.airbyte.config.Tag
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
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
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Singleton
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
  private val catalogMergeHelper: CatalogMergeHelper,
  private val applySchemaChangeHelper: ApplySchemaChangeHelper,
  private val apiPojoConverters: ApiPojoConverters,
  private val destinationCatalogGenerator: DestinationCatalogGenerator,
  private val connectionTimelineEventHelper: ConnectionTimelineEventHelper,
  private val catalogConfigDiffHelper: CatalogConfigDiffHelper,
) {
  fun getWorkspaceState(webBackendWorkspaceState: WebBackendWorkspaceState): WebBackendWorkspaceStateResult {
    val workspaceId = webBackendWorkspaceState.workspaceId
    val connectionCount = workspaceService.countConnectionsForWorkspace(workspaceId)
    val destinationCount = workspaceService.countDestinationsForWorkspace(workspaceId)
    val sourceCount = workspaceService.countSourcesForWorkspace(workspaceId)

    return WebBackendWorkspaceStateResult(
      hasConnections = connectionCount > 0,
      hasDestinations = destinationCount > 0,
      hasSources = sourceCount > 0,
    )
  }

  fun webBackendGetConnectionStatusCounts(workspaceIdRequestBody: WorkspaceIdRequestBody): WebBackendConnectionStatusCounts {
    val statusCounts = connectionService.getConnectionStatusCounts(workspaceIdRequestBody.workspaceId)
    return WebBackendConnectionStatusCounts(
      running = statusCounts.running,
      queued = statusCounts.queued,
      healthy = statusCounts.healthy,
      failed = statusCounts.failed,
      paused = statusCounts.paused,
      notSynced = statusCounts.notSynced,
    )
  }

  fun getStateType(connectionIdRequestBody: ConnectionIdRequestBody): ConnectionStateType =
    stateHandler.getState(connectionIdRequestBody).stateType.convertTo<ConnectionStateType>()

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
      connectionService.listWorkspaceStandardSyncsCursorPaginated(query, cursorPagination)
    val sourceIds = connectionsWithJobInfo.stream().map { conn: ConnectionWithJobInfo -> conn.connection().sourceId }.toList()
    val destinationIds = connectionsWithJobInfo.stream().map { conn: ConnectionWithJobInfo -> conn.connection().destinationId }.toList()

    // Fetching all the related objects we need for the final output
    val sourceReadById = getSourceSnippetReadById(sourceIds)
    val destinationReadById = getDestinationSnippetReadById(destinationIds)
    val newestFetchEventsByActorId =
      catalogService.getMostRecentActorCatalogFetchEventForSources(sourceIds)

    val connectionItems: MutableList<WebBackendConnectionListItem> = mutableListOf()

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

    return WebBackendConnectionReadList(
      connections = connectionItems,
      pageSize = pageSize,
      numConnections = numConnections,
    )
  }

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

  private fun buildWebBackendConnectionRead(
    connectionRead: ConnectionRead,
    currentSourceCatalogId: Optional<UUID>,
  ): WebBackendConnectionRead {
    val source = getSourceRead(connectionRead.sourceId)
    val destination = getDestinationRead(connectionRead.destinationId)
    val operations = getOperationReadList(connectionRead)
    val latestSyncJob = jobHistoryHandler.getLatestSyncJob(connectionRead.connectionId)
    val latestRunningSyncJob = jobHistoryHandler.getLatestRunningSyncJob(connectionRead.connectionId)

    val mostRecentFetchEvent =
      catalogService.getMostRecentActorCatalogFetchEventForSource(connectionRead.sourceId)

    val schemaChange = getSchemaChange(connectionRead, currentSourceCatalogId, mostRecentFetchEvent)

    // find any scheduled or past breaking changes to the connectors
    val sourceActorDefinitionVersionRead =
      actorDefinitionVersionHandler
        .getActorDefinitionVersionForSourceId(
          SourceIdRequestBody(sourceId = source.sourceId),
        )
    val destinationActorDefinitionVersionRead =
      actorDefinitionVersionHandler
        .getActorDefinitionVersionForDestinationId(
          DestinationIdRequestBody(destinationId = destination.destinationId),
        )

    return getWebBackendConnectionRead(
      connectionRead = connectionRead,
      source = source,
      destination = destination,
      operations = operations,
      isSyncing = latestRunningSyncJob.isPresent,
      schemaChange = schemaChange,
      sourceActorDefinitionVersion = sourceActorDefinitionVersionRead,
      destinationActorDefinitionVersion = destinationActorDefinitionVersionRead,
    ).copy(
      catalogId = connectionRead.sourceCatalogId,
      latestSyncJobCreatedAt = latestSyncJob.map { job: JobRead -> job.createdAt }.orElse(null),
      latestSyncJobStatus = latestSyncJob.map { job: JobRead -> job.status }.orElse(null),
    )
  }

  private fun buildWebBackendConnectionListItem(
    connectionWithJobInfo: ConnectionWithJobInfo,
    sourceReadById: Map<UUID, SourceSnippetRead>,
    destinationReadById: Map<UUID, DestinationSnippetRead>,
    latestFetchEvent: Optional<ActorCatalogFetchEvent>,
  ): WebBackendConnectionListItem {
    val source = sourceReadById[connectionWithJobInfo.connection().sourceId]
    val destination = destinationReadById[connectionWithJobInfo.connection().destinationId]
    val connectionRead = apiPojoConverters.internalToConnectionRead(connectionWithJobInfo.connection())
    val currentCatalogId = Optional.ofNullable(connectionRead.sourceCatalogId)

    val schemaChange = getSchemaChange(connectionRead, currentCatalogId, latestFetchEvent)

    // find any scheduled or past breaking changes to the connectors
    val sourceActorDefinitionVersionRead =
      actorDefinitionVersionHandler
        .getActorDefinitionVersionForSourceId(
          SourceIdRequestBody(sourceId = source!!.sourceId),
        )
    val destinationActorDefinitionVersionRead =
      actorDefinitionVersionHandler
        .getActorDefinitionVersionForDestinationId(
          DestinationIdRequestBody(destinationId = destination!!.destinationId),
        )

    val listItem =
      WebBackendConnectionListItem(
        connectionId = connectionWithJobInfo.connection().connectionId,
        status = apiPojoConverters.toApiStatus(connectionWithJobInfo.connection().status)!!,
        name = connectionWithJobInfo.connection().name,
        scheduleType = apiPojoConverters.toApiConnectionScheduleType(connectionWithJobInfo.connection()),
        scheduleData = apiPojoConverters.toApiConnectionScheduleData(connectionWithJobInfo.connection()),
        source = source,
        destination = destination,
        isSyncing =
          connectionWithJobInfo
            .latestJobStatus()
            .map { status: JobStatus -> status == JobStatus.running }
            .orElse(false),
        schemaChange = schemaChange,
        sourceActorDefinitionVersion = sourceActorDefinitionVersionRead,
        destinationActorDefinitionVersion = destinationActorDefinitionVersionRead,
        onDemandEnabled = connectionWithJobInfo.connection().onDemandEnabled ?: false,
        tags =
          connectionWithJobInfo
            .connection()
            .tags
            .stream()
            .map { tag: Tag -> this.buildTag(tag) }
            .toList(),
        latestSyncJobCreatedAt =
          connectionWithJobInfo
            .latestJobCreatedAt()
            .map { createdAt: OffsetDateTime ->
              createdAt.toEpochSecond()
            }.orElse(null),
        latestSyncJobStatus =
          connectionWithJobInfo
            .latestJobStatus()
            .map { status: JobStatus ->
              io.airbyte.api.server.generated.models.JobStatus.entries
                .first { it.value == status.literal }
            }.orElse(null),
      )

    return listItem
  }

  private fun buildTag(tag: Tag): io.airbyte.api.server.generated.models.Tag =
    io.airbyte.api.server.generated.models.Tag(
      tagId = tag.tagId,
      workspaceId = tag.workspaceId,
      name = tag.name,
      color = tag.color,
    )

  @WithSpan
  private fun getSourceRead(sourceId: UUID): SourceRead = sourceHandler.getSource(SourceIdRequestBody(sourceId = sourceId))

  @WithSpan
  private fun getDestinationRead(destinationId: UUID): DestinationRead =
    destinationHandler.getDestination(DestinationIdRequestBody(destinationId = destinationId))

  @WithSpan
  private fun getOperationReadList(connectionRead: ConnectionRead): OperationReadList {
    val connectionIdRequestBody = ConnectionIdRequestBody(connectionId = connectionRead.connectionId)
    return operationsHandler.listOperationsForConnection(connectionIdRequestBody)
  }

  // todo (cgardens) - This logic is a headache to follow it stems from the internal data model not
  // tracking selected streams in any reasonable way. We should update that.
  @WithSpan
  fun webBackendGetConnection(webBackendConnectionRequestBody: WebBackendConnectionRequestBody): WebBackendConnectionRead {
    addTagsToTrace(mapOf(MetricTags.CONNECTION_ID to webBackendConnectionRequestBody.connectionId.toString()))
    val connectionIdRequestBody =
      ConnectionIdRequestBody(connectionId = webBackendConnectionRequestBody.connectionId)

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
      if (webBackendConnectionRequestBody.withRefreshedCatalog == true) {
        getRefreshedSchema(connection.sourceId, connection.connectionId)
      } else {
        Optional.empty()
      }

    val diff: CatalogDiff?
    val syncCatalog: AirbyteCatalog
    val currentSourceCatalogId = Optional.ofNullable(connection.sourceCatalogId)
    var refreshedConnection = connection
    // A refreshed catalog with a null catalog means the discover job failed; fall back to the
    // existing catalog instead of crashing on the null catalog below.
    val refreshedDiscoveredCatalog = refreshedCatalog.map { it.catalog }.orElse(null)
    if (refreshedDiscoveredCatalog != null) {
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
        catalogMergeHelper.mergeCatalogWithConfiguration(
          configuredCatalog,
          catalogUsedToMakeConfiguredCatalog.orElse(configuredCatalog),
          refreshedDiscoveredCatalog,
        )

      diff = refreshedCatalog.get().catalogDiff
      refreshedConnection =
        connection.copy(
          sourceCatalogId = refreshedCatalog.get().catalogId,
          breakingChange = refreshedCatalog.get().breakingChange!!,
          status = refreshedCatalog.get().connectionStatus!!,
        )
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

    return buildWebBackendConnectionRead(refreshedConnection.copy(syncCatalog = syncCatalog), currentSourceCatalogId).copy(catalogDiff = diff)
  }

  private fun updateSchemaWithOriginalDiscoveredCatalog(
    configuredCatalog: AirbyteCatalog,
    originalDiscoveredCatalog: AirbyteCatalog,
  ): AirbyteCatalog {
    // We pass the original discovered catalog in as the "new" discovered catalog.
    return catalogMergeHelper.mergeCatalogWithConfiguration(configuredCatalog, originalDiscoveredCatalog, originalDiscoveredCatalog)
  }

  private fun getRefreshedSchema(
    sourceId: UUID,
    connectionId: UUID,
  ): Optional<SourceDiscoverSchemaRead> {
    val discoverSchemaReadReq =
      SourceDiscoverSchemaRequestBody(
        sourceId = sourceId,
        disableCache = true,
        connectionId = connectionId,
      )
    val schemaRead = schedulerHandler.discoverSchemaForSourceFromSourceId(discoverSchemaReadReq)
    return Optional.ofNullable(schemaRead)
  }

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
  fun webBackendUpdateConnection(webBackendConnectionPatch: WebBackendConnectionUpdate): WebBackendConnectionRead {
    val connectionId = webBackendConnectionPatch.connectionId
    val originalConnectionRead = connectionsHandler.getConnection(connectionId)
    var breakingChange = originalConnectionRead.breakingChange
    val source = getSourceRead(originalConnectionRead.sourceId)

    // If there have been changes to the sync catalog, check whether these changes result in or fix a
    // broken connection
    var catalogConfigDiff: CatalogConfigDiff? = null // Config diff
    var schemaDiff: CatalogDiff? = null // Schema diff
    val newAirbyteCatalog = webBackendConnectionPatch.syncCatalog
    if (newAirbyteCatalog != null) {
      // Get the most recent actor catalog fetched for this connection's source and the newly updated sync
      // catalog
      val mostRecentActorCatalog =
        catalogService.getMostRecentActorCatalogForSource(originalConnectionRead.sourceId)
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
    var updatedConnectionRead = connectionsHandler.updateConnection(connectionPatch, null, false)
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
        catalogMergeHelper.mergeCatalogWithConfiguration(
          updatedConnectionRead.syncCatalog,
          catalogUsedToMakeConfiguredCatalog.get(),
          catalogUsedToMakeConfiguredCatalog.get(),
        )
      updatedConnectionRead = updatedConnectionRead.copy(syncCatalog = syncCatalog)
    }

    val currentSourceCatalogId = Optional.ofNullable(updatedConnectionRead.sourceCatalogId)
    return buildWebBackendConnectionRead(updatedConnectionRead, currentSourceCatalogId)
  }

  /**
   * Given a fully updated connection, check for a diff between the old catalog and the updated
   * catalog to see if any streams need to be reset.
   */
  private fun resetStreamsIfNeeded(
    webBackendConnectionPatch: WebBackendConnectionUpdate,
    oldConfiguredCatalog: ConfiguredAirbyteCatalog,
    updatedConnectionRead: ConnectionRead,
    oldConnectionRead: ConnectionRead,
  ) {
    val connectionId = webBackendConnectionPatch.connectionId
    val skipReset = webBackendConnectionPatch.skipReset ?: false
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
          .getActorDefinitionVersionForDestinationId(
            DestinationIdRequestBody(destinationId = oldConnectionRead.destinationId),
          )
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

  private fun createOperations(webBackendConnectionCreate: WebBackendConnectionCreate): List<UUID> {
    val operations = webBackendConnectionCreate.operations ?: return emptyList()
    val operationIds: MutableList<UUID> = ArrayList()
    for (operationCreate in operations) {
      operationIds.add(operationsHandler.createOperation(operationCreate).operationId)
    }
    return operationIds
  }

  private fun createOrUpdateOperations(
    connectionRead: ConnectionRead,
    webBackendConnectionPatch: WebBackendConnectionUpdate,
  ): List<UUID>? {
    // this is a patch-style update, so don't make any changes if the request doesn't include operations

    val updatedOperations = webBackendConnectionPatch.operations ?: return null

    // wrap operationIds in a new ArrayList so that it is modifiable below, when calling .removeAll
    val originalOperationIds: MutableList<UUID> = ArrayList(connectionRead.operationIds.orEmpty())

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

  companion object {
        /*
         * A breakingChange boolean is stored on the connectionRead object and corresponds to the boolean
         * breakingChange field on the connection table. If there is not a breaking change, we still have to
         * check whether there is a non-breaking schema change by fetching the most recent
         * ActorCatalogFetchEvent. A new ActorCatalogFetchEvent is stored each time there is a source schema
         * refresh, so if the most recent ActorCatalogFetchEvent has a different actor catalog than the
         * existing actor catalog, there is a schema change.
         */
    @InternalForTesting
    @JvmStatic
    fun getSchemaChange(
      connectionRead: ConnectionRead?,
      currentSourceCatalogId: Optional<UUID>,
      mostRecentFetchEvent: Optional<ActorCatalogFetchEvent>,
    ): SchemaChange {
      if (connectionRead == null || currentSourceCatalogId.isEmpty) {
        return SchemaChange.NO_CHANGE
      }

      if (connectionRead.breakingChange) {
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
      isSyncing: Boolean,
      schemaChange: SchemaChange,
      sourceActorDefinitionVersion: ActorDefinitionVersionRead,
      destinationActorDefinitionVersion: ActorDefinitionVersionRead,
    ): WebBackendConnectionRead =
      WebBackendConnectionRead(
        connectionId = connectionRead.connectionId,
        sourceId = connectionRead.sourceId,
        destinationId = connectionRead.destinationId,
        operationIds = connectionRead.operationIds,
        name = connectionRead.name,
        namespaceDefinition = connectionRead.namespaceDefinition,
        namespaceFormat = connectionRead.namespaceFormat,
        prefix = connectionRead.prefix,
        syncCatalog = connectionRead.syncCatalog,
        destinationCatalogId = connectionRead.destinationCatalogId,
        status = connectionRead.status,
        schedule = connectionRead.schedule,
        scheduleType = connectionRead.scheduleType,
        scheduleData = connectionRead.scheduleData,
        source = source,
        destination = destination,
        operations = operations.operations,
        dataplaneGroupId = connectionRead.dataplaneGroupId,
        resourceRequirements = connectionRead.resourceRequirements,
        // read from non-null connection columns
        notifySchemaChanges = connectionRead.notifySchemaChanges!!,
        notifySchemaChangesByEmail = connectionRead.notifySchemaChangesByEmail!!,
        createdAt = connectionRead.createdAt,
        nonBreakingChangesPreference = connectionRead.nonBreakingChangesPreference!!,
        backfillPreference = connectionRead.backfillPreference,
        tags = connectionRead.tags!!,
        onDemandEnabled = connectionRead.onDemandEnabled ?: false,
        isSyncing = isSyncing,
        schemaChange = schemaChange,
        sourceActorDefinitionVersion = sourceActorDefinitionVersion,
        destinationActorDefinitionVersion = destinationActorDefinitionVersion,
      )

    @InternalForTesting
    protected fun toOperationCreate(operationCreateOrUpdate: WebBackendOperationCreateOrUpdate): OperationCreate =
      OperationCreate(
        name = operationCreateOrUpdate.name,
        workspaceId = operationCreateOrUpdate.workspaceId,
        operatorConfiguration = operationCreateOrUpdate.operatorConfiguration,
      )

    @JvmStatic
    @InternalForTesting
    fun toOperationUpdate(operationCreateOrUpdate: WebBackendOperationCreateOrUpdate): OperationUpdate =
      OperationUpdate(
        operationId = operationCreateOrUpdate.operationId!!,
        name = operationCreateOrUpdate.name,
        operatorConfiguration = operationCreateOrUpdate.operatorConfiguration,
      )

    /**
     * Note: [ConnectionCreate.enableIndexing] is deliberately left unset here. `WebBackendConnectionCreate`
     * has no equivalent field -- Fusion indexing is set through connection create/update directly, or
     * through `/v1/connections/{id}/enablement`, not the webapp's create form.
     */
    @InternalForTesting
    @JvmStatic
    fun toConnectionCreate(
      webBackendConnectionCreate: WebBackendConnectionCreate,
      operationIds: List<UUID>,
    ): ConnectionCreate =
      ConnectionCreate(
        name = webBackendConnectionCreate.name,
        namespaceDefinition = webBackendConnectionCreate.namespaceDefinition,
        namespaceFormat = webBackendConnectionCreate.namespaceFormat,
        prefix = webBackendConnectionCreate.prefix,
        sourceId = webBackendConnectionCreate.sourceId,
        destinationId = webBackendConnectionCreate.destinationId,
        operationIds = operationIds,
        syncCatalog = webBackendConnectionCreate.syncCatalog,
        schedule = webBackendConnectionCreate.schedule,
        scheduleType = webBackendConnectionCreate.scheduleType,
        scheduleData = webBackendConnectionCreate.scheduleData,
        status = webBackendConnectionCreate.status,
        resourceRequirements = webBackendConnectionCreate.resourceRequirements,
        sourceCatalogId = webBackendConnectionCreate.sourceCatalogId,
        destinationCatalogId = webBackendConnectionCreate.destinationCatalogId,
        dataplaneGroupId = webBackendConnectionCreate.dataplaneGroupId,
        notifySchemaChanges = webBackendConnectionCreate.notifySchemaChanges,
        nonBreakingChangesPreference = webBackendConnectionCreate.nonBreakingChangesPreference,
        backfillPreference = webBackendConnectionCreate.backfillPreference,
        tags = webBackendConnectionCreate.tags,
        onDemandEnabled = webBackendConnectionCreate.onDemandEnabled,
      )

    /**
     * Take in a WebBackendConnectionUpdate and convert it into a ConnectionUpdate. OperationIds are
     * handled as a special case because the WebBackendConnectionUpdate handler allows for on-the-fly
     * creation of new operations. So, the brand-new IDs are passed in because they aren't present in
     * the WebBackendConnectionUpdate itself.
     *
     * The return value is used as a patch -- a field set to null means that it should not be modified.
     *
     * Note: [ConnectionUpdate.enableIndexing] is deliberately left unset (null = "no change") here.
     * `WebBackendConnectionUpdate` has no equivalent field -- Fusion indexing is set through connection
     * create/update directly, or through `/v1/connections/{id}/enablement`, not the webapp's edit form.
     */
    @InternalForTesting
    @JvmStatic
    fun toConnectionPatch(
      webBackendConnectionPatch: WebBackendConnectionUpdate,
      finalOperationIds: List<UUID>?,
      breakingChange: Boolean,
    ): ConnectionUpdate =
      ConnectionUpdate(
        connectionId = webBackendConnectionPatch.connectionId,
        namespaceDefinition = webBackendConnectionPatch.namespaceDefinition,
        namespaceFormat = webBackendConnectionPatch.namespaceFormat,
        prefix = webBackendConnectionPatch.prefix,
        name = webBackendConnectionPatch.name,
        syncCatalog = webBackendConnectionPatch.syncCatalog,
        schedule = webBackendConnectionPatch.schedule,
        scheduleType = webBackendConnectionPatch.scheduleType,
        scheduleData = webBackendConnectionPatch.scheduleData,
        status = webBackendConnectionPatch.status,
        resourceRequirements = webBackendConnectionPatch.resourceRequirements,
        sourceCatalogId = webBackendConnectionPatch.sourceCatalogId,
        destinationCatalogId = webBackendConnectionPatch.destinationCatalogId,
        dataplaneGroupId = webBackendConnectionPatch.dataplaneGroupId,
        notifySchemaChanges = webBackendConnectionPatch.notifySchemaChanges,
        notifySchemaChangesByEmail = webBackendConnectionPatch.notifySchemaChangesByEmail,
        nonBreakingChangesPreference = webBackendConnectionPatch.nonBreakingChangesPreference,
        backfillPreference = webBackendConnectionPatch.backfillPreference,
        breakingChange = breakingChange,
        tags = webBackendConnectionPatch.tags,
        onDemandEnabled = webBackendConnectionPatch.onDemandEnabled,
        enableIndexing = null,
        operationIds = finalOperationIds,
      )

    @JvmStatic
    @InternalForTesting
    fun getStreamsToReset(catalogDiff: CatalogDiff): List<StreamDescriptor> =
      catalogDiff.transforms
        .stream()
        .map { obj: StreamTransform -> obj.streamDescriptor }
        .toList()
  }
}
