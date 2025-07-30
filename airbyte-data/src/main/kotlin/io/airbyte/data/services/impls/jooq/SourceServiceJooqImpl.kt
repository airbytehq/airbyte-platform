/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.data.services.shared.Filters
import io.airbyte.data.services.shared.ResourcesQueryPaginated
import io.airbyte.data.services.shared.SortKey
import io.airbyte.data.services.shared.SourceAndDefinition
import io.airbyte.data.services.shared.SourceConnectionWithCount
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.SourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.metrics.MetricClient
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.JSONB
import org.jooq.JoinType
import org.jooq.Record
import org.jooq.Record1
import org.jooq.Result
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Map.entry
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors
import java.util.stream.Stream

@Singleton
class SourceServiceJooqImpl(
  @Named("configDatabase") database: Database,
  featureFlagClient: FeatureFlagClient,
  secretPersistenceConfigService: SecretPersistenceConfigService,
  connectionService: ConnectionService,
  actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater,
  metricClient: MetricClient,
  actorPaginationServiceHelper: ActorServicePaginationHelper,
) : SourceService {
  private val database: ExceptionWrappingDatabase
  private val featureFlagClient: FeatureFlagClient
  private val secretPersistenceConfigService: SecretPersistenceConfigService
  private val connectionService: ConnectionService
  private val actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater
  private val metricClient: MetricClient
  private val actorPaginationServiceHelper: ActorServicePaginationHelper

  // TODO: This has too many dependencies.
  init {
    this.database = ExceptionWrappingDatabase(database)
    this.connectionService = connectionService
    this.featureFlagClient = featureFlagClient
    this.secretPersistenceConfigService = secretPersistenceConfigService
    this.actorDefinitionVersionUpdater = actorDefinitionVersionUpdater
    this.metricClient = metricClient
    this.actorPaginationServiceHelper = actorPaginationServiceHelper
  }

  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  override fun getStandardSourceDefinition(sourceDefinitionId: UUID): StandardSourceDefinition = getStandardSourceDefinition(sourceDefinitionId, true)

  /**
   * Get source definition.
   *
   * @param sourceDefinitionId source definition id
   * @return source definition
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  override fun getStandardSourceDefinition(
    sourceDefinitionId: UUID,
    includeTombstones: Boolean,
  ): StandardSourceDefinition =
    sourceDefQuery(Optional.of(sourceDefinitionId), includeTombstones)
      .findFirst()
      .orElseThrow { ConfigNotFoundException(ConfigNotFoundType.STANDARD_SOURCE_DEFINITION, sourceDefinitionId) }

  /**
   * Get source definition form source.
   *
   * @param sourceId source id
   * @return source definition
   */
  override fun getSourceDefinitionFromSource(sourceId: UUID): StandardSourceDefinition {
    try {
      val source = getSourceConnection(sourceId)
      return getStandardSourceDefinition(source.getSourceDefinitionId())
    } catch (e: Exception) {
      throw RuntimeException(e)
    }
  }

  /**
   * Get source definition used by a connection.
   *
   * @param connectionId connection id
   * @return source definition
   */
  override fun getSourceDefinitionFromConnection(connectionId: UUID): StandardSourceDefinition {
    try {
      // TODO: This should be refactored to use the repository. Services should not depend on other
      // services.
      val sync = connectionService.getStandardSync(connectionId)
      return getSourceDefinitionFromSource(sync.getSourceId())
    } catch (e: Exception) {
      throw RuntimeException(e)
    }
  }

  /**
   * List standard source definitions.
   *
   * @param includeTombstone include tombstoned source
   * @return list source definitions
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun listStandardSourceDefinitions(includeTombstone: Boolean): MutableList<StandardSourceDefinition> =
    sourceDefQuery(Optional.empty<UUID?>(), includeTombstone).toList()

  /**
   * List public source definitions.
   *
   * @param includeTombstone include tombstoned source
   * @return public source definitions
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun listPublicSourceDefinitions(includeTombstone: Boolean): MutableList<StandardSourceDefinition> =
    listStandardActorDefinitions(
      ActorType.source,
      { record: Record ->
        DbConverter.buildStandardSourceDefinition(
          record,
          retrieveDefaultMaxSecondsBetweenMessages(
            record.get(
              Tables.ACTOR_DEFINITION.ID,
            ),
          ),
        )
      },
      includeTombstones(Tables.ACTOR_DEFINITION.TOMBSTONE, includeTombstone),
      Tables.ACTOR_DEFINITION.PUBLIC.eq(true),
    )

  /**
   * List source definitions used by the given workspace.
   *
   * @param workspaceId workspace id
   * @param includeTombstone include tombstoned sources
   * @return source definitions used by workspace
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun listSourceDefinitionsForWorkspace(
    workspaceId: UUID,
    includeTombstone: Boolean,
  ): List<StandardSourceDefinition> =
    database
      .query<Result<Record>> { ctx: DSLContext ->
        ctx
          .selectDistinct(Tables.ACTOR_DEFINITION.asterisk())
          .from(Tables.ACTOR_DEFINITION)
          .join(Tables.ACTOR)
          .on(Tables.ACTOR.ACTOR_DEFINITION_ID.eq(Tables.ACTOR_DEFINITION.ID))
          .where(Tables.ACTOR.WORKSPACE_ID.eq(workspaceId))
          .and(Tables.ACTOR_DEFINITION.ACTOR_TYPE.eq(ActorType.source))
          .and(if (includeTombstone) DSL.noCondition() else Tables.ACTOR.TOMBSTONE.notEqual(true))
          .and(if (includeTombstone) DSL.noCondition() else Tables.ACTOR_DEFINITION.TOMBSTONE.notEqual(true))
          .fetch()
      }.stream()
      .map { record: Record ->
        DbConverter.buildStandardSourceDefinition(
          record,
          retrieveDefaultMaxSecondsBetweenMessages(
            record.get(Tables.ACTOR_DEFINITION.ID),
          ),
        )
      }.toList()

  /**
   * List granted source definitions for workspace.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombstoned destinations
   * @return list standard source definitions
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun listGrantedSourceDefinitions(
    workspaceId: UUID,
    includeTombstones: Boolean,
  ): MutableList<StandardSourceDefinition> =
    listActorDefinitionsJoinedWithGrants(
      workspaceId,
      ScopeType.workspace,
      JoinType.JOIN,
      ActorType.source,
      { record: Record ->
        DbConverter.buildStandardSourceDefinition(
          record,
          retrieveDefaultMaxSecondsBetweenMessages(
            record.get(
              Tables.ACTOR_DEFINITION.ID,
            ),
          ),
        )
      },
      includeTombstones(Tables.ACTOR_DEFINITION.TOMBSTONE, includeTombstones),
    )

  /**
   * List source to which we can give a grant.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombstoned definitions
   * @return list of pairs from source definition and whether it can be granted
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun listGrantableSourceDefinitions(
    workspaceId: UUID,
    includeTombstones: Boolean,
  ): MutableList<MutableMap.MutableEntry<StandardSourceDefinition, Boolean>> =
    listActorDefinitionsJoinedWithGrants(
      workspaceId,
      ScopeType.workspace,
      JoinType.LEFT_OUTER_JOIN,
      ActorType.source,
      { record: Record -> this.mapRecordToSourceDefinitionWithGrantStatus(record!!) },
      Tables.ACTOR_DEFINITION.CUSTOM.eq(false),
      includeTombstones(Tables.ACTOR_DEFINITION.TOMBSTONE, includeTombstones),
    )

  private fun mapRecordToSourceDefinitionWithGrantStatus(record: Record): MutableMap.MutableEntry<StandardSourceDefinition, Boolean> =
    actorDefinitionWithGrantStatus(record, { rec: Record -> buildStandardSourceDefinition(rec!!) })

  private fun buildStandardSourceDefinition(record: Record): StandardSourceDefinition {
    // Extract the StandardSourceDefinition from the record using the default max seconds logic.
    return DbConverter.buildStandardSourceDefinition(
      record,
      retrieveDefaultMaxSecondsBetweenMessages(record.get(Tables.ACTOR_DEFINITION.ID)),
    )
  }

  /**
   * Update source definition.
   *
   * @param sourceDefinition source definition
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  override fun updateStandardSourceDefinition(sourceDefinition: StandardSourceDefinition) {
    // Check existence before updating
    // TODO: split out write and update methods so that we don't need explicit checking
    getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId())

    database.transaction<Any?>(
      { ctx: DSLContext? ->
        writeStandardSourceDefinition(mutableListOf(sourceDefinition), ctx!!)
        null
      },
    )
  }

  /**
   * Returns source with a given id. Does not contain secrets.
   *
   * @param sourceId - id of source to fetch.
   * @return sources
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  override fun getSourceConnection(sourceId: UUID): SourceConnection {
    val sourceConnection =
      listSourceQuery(Optional.of(sourceId))
        .findFirst()
        .orElseThrow({ ConfigNotFoundException(ConfigNotFoundType.SOURCE_CONNECTION, sourceId) })

    return sourceConnection
  }

  /**
   * List sources for a workspace along with connection counts
   *
   * @param workspaceId - The workspace ID
   * @param workspaceResourceCursorPagination - the cursor object for paginating over results
   * @throws IOException - you never know when you IO
   */
  override fun listWorkspaceSourceConnectionsWithCounts(
    workspaceId: UUID,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
  ): List<SourceConnectionWithCount> =
    actorPaginationServiceHelper
      .listWorkspaceActorConnectionsWithCounts(
        workspaceId,
        workspaceResourceCursorPagination,
        ActorType.source,
      ).map { actorConnectionWithCount ->
        actorConnectionWithCount.sourceConnection?.let { source ->
          SourceConnectionWithCount(
            source,
            actorConnectionWithCount.actorDefinitionName,
            actorConnectionWithCount.connectionCount,
            actorConnectionWithCount.lastSync,
            actorConnectionWithCount.connectionJobStatuses,
            actorConnectionWithCount.isActive,
          )
        } ?: throw IllegalStateException("Expected source connection for source actor type")
      }

  /**
   * Get the count of sources for a workspace
   *
   * @param workspaceId - The workspace ID
   * @param workspaceResourceCursorPagination - the cursor object for paginating over results
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun countWorkspaceSourcesFiltered(
    workspaceId: UUID,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
  ): Int = actorPaginationServiceHelper.countWorkspaceActorsFiltered(workspaceId, workspaceResourceCursorPagination, ActorType.source)

  override fun buildCursorPagination(
    cursor: UUID?,
    internalSortKey: SortKey,
    filters: Filters?,
    ascending: Boolean?,
    pageSize: Int?,
  ): WorkspaceResourceCursorPagination? =
    actorPaginationServiceHelper.buildCursorPagination(cursor, internalSortKey, filters, ascending, pageSize, ActorType.source)

  /**
   * Write a SourceConnection to the database. The configuration of the Source should be a partial
   * configuration (no secrets, just pointer to the secrets store).
   *
   * @param partialSource - The configuration of the Source will be a partial configuration (no
   * secrets, just pointer to the secrets store)
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun writeSourceConnectionNoSecrets(partialSource: SourceConnection) {
    database.transaction<Any?>(
      { ctx: DSLContext? ->
        writeSourceConnection(mutableListOf(partialSource), ctx!!)
        null
      },
    )
  }

  /**
   * Returns all sources in the database. Does not contain secrets.
   *
   * @return sources
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun listSourceConnection(): MutableList<SourceConnection> = listSourceQuery(Optional.empty<UUID>()).toList()

  /**
   * Returns all sources for a workspace. Does not contain secrets.
   *
   * @param workspaceId - id of the workspace
   * @return sources
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun listWorkspaceSourceConnection(workspaceId: UUID): MutableList<SourceConnection> {
    val result =
      database.query<Result<Record>>(
        { ctx: DSLContext? ->
          ctx!!
            .select(DSL.asterisk())
            .from(Tables.ACTOR)
            .where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.source))
            .and(Tables.ACTOR.WORKSPACE_ID.eq(workspaceId))
            .andNot(Tables.ACTOR.TOMBSTONE)
            .fetch()
        },
      )
    return result.stream().map { record: Record -> DbConverter.buildSourceConnection(record) }.collect(Collectors.toList())
  }

  /**
   * Returns if a source is active, i.e. the source has at least one active or manual connection.
   *
   * @param sourceId - id of the source
   * @return boolean - if source is active or not
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun isSourceActive(sourceId: UUID): Boolean =
    database.query(
      { ctx: DSLContext? ->
        ctx!!.fetchExists(
          DSL
            .select()
            .from(Tables.CONNECTION)
            .where(Tables.CONNECTION.SOURCE_ID.eq(sourceId))
            .and(Tables.CONNECTION.STATUS.eq(StatusType.active)),
        )
      },
    )

  /**
   * Returns all sources for a set of workspaces. Does not contain secrets.
   *
   * @param resourcesQueryPaginated - Includes all the things we might want to query
   * @return sources
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun listWorkspacesSourceConnections(resourcesQueryPaginated: ResourcesQueryPaginated): MutableList<SourceConnection> {
    val result =
      database.query<Result<Record>>(
        { ctx: DSLContext? ->
          ctx!!
            .select(DSL.asterisk())
            .from(Tables.ACTOR)
            .where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.source))
            .and(Tables.ACTOR.WORKSPACE_ID.`in`(resourcesQueryPaginated.workspaceIds))
            .and(if (resourcesQueryPaginated.includeDeleted) DSL.noCondition() else Tables.ACTOR.TOMBSTONE.notEqual(true))
            .limit(resourcesQueryPaginated.pageSize)
            .offset(resourcesQueryPaginated.rowOffset)
            .fetch()
        },
      )
    return result.stream().map { record: Record -> DbConverter.buildSourceConnection(record) }.collect(Collectors.toList())
  }

  /**
   * Returns all active sources using a definition.
   *
   * @param definitionId - id for the definition
   * @return sources
   * @throws IOException - exception while interacting with the db
   */
  @Throws(IOException::class)
  override fun listSourcesForDefinition(definitionId: UUID): MutableList<SourceConnection> {
    val result =
      database.query<Result<Record>>(
        { ctx: DSLContext? ->
          ctx!!
            .select(DSL.asterisk())
            .from(Tables.ACTOR)
            .where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.source))
            .and(Tables.ACTOR.ACTOR_DEFINITION_ID.eq(definitionId))
            .andNot(Tables.ACTOR.TOMBSTONE)
            .fetch()
        },
      )
    return result.stream().map { record: Record -> DbConverter.buildSourceConnection(record) }.collect(Collectors.toList())
  }

  /**
   * Get source and definition from sources ids.
   *
   * @param sourceIds source ids
   * @return pair of source and definition
   * @throws IOException if there is an issue while interacting with db.
   */
  @Throws(IOException::class)
  override fun getSourceAndDefinitionsFromSourceIds(sourceIds: List<UUID>): List<SourceAndDefinition> {
    val records =
      database.query<Result<Record>>(
        { ctx: DSLContext? ->
          ctx!!
            .select(Tables.ACTOR.asterisk(), Tables.ACTOR_DEFINITION.asterisk())
            .from(Tables.ACTOR)
            .join(Tables.ACTOR_DEFINITION)
            .on(Tables.ACTOR.ACTOR_DEFINITION_ID.eq(Tables.ACTOR_DEFINITION.ID))
            .where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.source), Tables.ACTOR.ID.`in`(sourceIds))
            .fetch()
        },
      )

    val sourceAndDefinitions: MutableList<SourceAndDefinition> = ArrayList()

    for (record in records) {
      val source = DbConverter.buildSourceConnection(record)
      val definition =
        DbConverter.buildStandardSourceDefinition(
          record,
          retrieveDefaultMaxSecondsBetweenMessages(source.getSourceDefinitionId()),
        )
      sourceAndDefinitions.add(SourceAndDefinition(source, definition))
    }

    return sourceAndDefinitions
  }

  /**
   * Write metadata for a source connector. Writes global metadata (source definition, breaking
   * changes) and versioned metadata (info for actor definition version to set as default). Sets the
   * new version as the default version and updates actors accordingly, based on whether the upgrade
   * will be breaking or not.
   *
   * @param sourceDefinition standard source definition
   * @param actorDefinitionVersion actor definition version, containing tag to set as default
   * @param breakingChangesForDefinition - list of breaking changes for the definition
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun writeConnectorMetadata(
    sourceDefinition: StandardSourceDefinition,
    actorDefinitionVersion: ActorDefinitionVersion,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
  ) {
    database.transaction<Any?>(
      { ctx: DSLContext? ->
        writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, breakingChangesForDefinition, ctx!!)
        null
      },
    )

    // FIXME(pedro): this should be moved out of this service
    actorDefinitionVersionUpdater.updateSourceDefaultVersion(sourceDefinition, actorDefinitionVersion, breakingChangesForDefinition)
  }

  @Throws(IOException::class)
  override fun writeCustomConnectorMetadata(
    sourceDefinition: StandardSourceDefinition,
    defaultVersion: ActorDefinitionVersion,
    scopeId: UUID,
    scopeType: io.airbyte.config.ScopeType,
  ) {
    database.transaction<Any?>(
      { ctx: DSLContext? ->
        writeConnectorMetadata(sourceDefinition, defaultVersion, listOf(), ctx!!)
        writeActorDefinitionWorkspaceGrant(
          sourceDefinition.getSourceDefinitionId(),
          scopeId,
          ScopeType.valueOf(scopeType.toString()),
          ctx,
        )
        null
      },
    )

    actorDefinitionVersionUpdater.updateSourceDefaultVersion(sourceDefinition, defaultVersion, mutableListOf())
  }

  @Throws(IOException::class)
  override fun listSourcesWithIds(sourceIds: List<UUID>): List<SourceConnection> {
    val result =
      database.query<Result<Record>>(
        { ctx: DSLContext? ->
          ctx!!
            .select(DSL.asterisk())
            .from(Tables.ACTOR)
            .where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.source))
            .and(Tables.ACTOR.ID.`in`(sourceIds))
            .andNot(Tables.ACTOR.TOMBSTONE)
            .fetch()
        },
      )
    return result.stream().map { record: Record -> DbConverter.buildSourceConnection(record) }.toList()
  }

  /**
   * Retrieve from Launch Darkly the default max seconds between messages for a given source. This
   * allows us to dynamically change the default max seconds between messages for a source.
   *
   * @param sourceDefinitionId to retrieve the default max seconds between messages for.
   * @return
   */
  @VisibleForTesting
  fun retrieveDefaultMaxSecondsBetweenMessages(sourceDefinitionId: UUID): Long =
    featureFlagClient.stringVariation(HeartbeatMaxSecondsBetweenMessages, SourceDefinition(sourceDefinitionId)).toLong()

  private fun writeActorDefinitionWorkspaceGrant(
    actorDefinitionId: UUID,
    scopeId: UUID,
    scopeType: ScopeType,
    ctx: DSLContext,
  ): Int {
    var insertStep =
      ctx
        .insertInto(
          Tables.ACTOR_DEFINITION_WORKSPACE_GRANT,
        ).set(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID, actorDefinitionId)
        .set(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE, scopeType)
        .set(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID, scopeId)
    // todo remove when we drop the workspace_id column
    if (scopeType == ScopeType.workspace) {
      insertStep = insertStep.set(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.WORKSPACE_ID, scopeId)
    }
    return insertStep.execute()
  }

  private fun writeConnectorMetadata(
    sourceDefinition: StandardSourceDefinition?,
    actorDefinitionVersion: ActorDefinitionVersion,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
    ctx: DSLContext,
  ) {
    writeStandardSourceDefinition(mutableListOf(sourceDefinition), ctx)
    ConnectorMetadataJooqHelper.writeActorDefinitionBreakingChanges(breakingChangesForDefinition, ctx)
    ConnectorMetadataJooqHelper.writeActorDefinitionVersion(actorDefinitionVersion, ctx)
  }

  @Throws(IOException::class)
  private fun sourceDefQuery(
    sourceDefId: Optional<UUID>,
    includeTombstone: Boolean,
  ): Stream<StandardSourceDefinition> =
    database
      .query<Result<Record>>(
        { ctx: DSLContext? ->
          ctx!!
            .select(Tables.ACTOR_DEFINITION.asterisk())
            .from(Tables.ACTOR_DEFINITION)
            .where(Tables.ACTOR_DEFINITION.ACTOR_TYPE.eq(ActorType.source))
            .and(sourceDefId.map { t: UUID? -> Tables.ACTOR_DEFINITION.ID.eq(t) }.orElse(DSL.noCondition()))
            .and(if (includeTombstone) DSL.noCondition() else Tables.ACTOR_DEFINITION.TOMBSTONE.notEqual(true))
            .fetch()
        },
      ).stream()
      .map { record: Record ->
        DbConverter.buildStandardSourceDefinition(
          record,
          retrieveDefaultMaxSecondsBetweenMessages(
            sourceDefId.orElse(ANONYMOUS)!!,
          ),
        )
      }

  @Throws(IOException::class)
  private fun <T> listStandardActorDefinitions(
    actorType: ActorType,
    recordToActorDefinition: Function<Record, T>,
    vararg conditions: Condition?,
  ): MutableList<T> {
    val records =
      database.query<Result<Record>>(
        { ctx: DSLContext? ->
          ctx!!
            .select(DSL.asterisk())
            .from(Tables.ACTOR_DEFINITION)
            .where(*conditions)
            .and(Tables.ACTOR_DEFINITION.ACTOR_TYPE.eq(actorType))
            .fetch()
        },
      )

    return records
      .stream()
      .map<T>(recordToActorDefinition)
      .toList()
  }

  @Throws(IOException::class)
  private fun <T> listActorDefinitionsJoinedWithGrants(
    scopeId: UUID,
    scopeType: ScopeType,
    joinType: JoinType,
    actorType: ActorType,
    recordToReturnType: Function<Record, T>,
    vararg conditions: Condition,
  ): MutableList<T> {
    val records =
      actorDefinitionsJoinedWithGrants(
        scopeId,
        scopeType,
        joinType,
        *ConditionsHelper.addAll(
          *conditions,
          Tables.ACTOR_DEFINITION.ACTOR_TYPE.eq(actorType),
          Tables.ACTOR_DEFINITION.PUBLIC.eq(false),
        ),
      )

    return records
      .stream()
      .map(recordToReturnType)
      .toList()
  }

  @Throws(IOException::class)
  private fun actorDefinitionsJoinedWithGrants(
    scopeId: UUID,
    scopeType: ScopeType,
    joinType: JoinType,
    vararg conditions: Condition,
  ): Result<Record> {
    var scopeConditional =
      Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE
        .eq(
          ScopeType.valueOf(scopeType.toString()),
        ).and(
          Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(scopeId),
        )

    // if scope type is workspace, get organization id as well and add that into OR conditional
    if (scopeType == ScopeType.workspace) {
      val organizationId = getOrganizationIdFromWorkspaceId(scopeId)
      if (organizationId.isPresent()) {
        scopeConditional =
          scopeConditional.or(
            Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE
              .eq(
                ScopeType.organization,
              ).and(
                Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(organizationId.get()),
              ),
          )
      }
    }

    val finalScopeConditional = scopeConditional
    return database.query<Result<Record>>(
      { ctx: DSLContext? ->
        ctx!!
          .select(DSL.asterisk())
          .from(Tables.ACTOR_DEFINITION)
          .join(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT, joinType)
          .on(
            Tables.ACTOR_DEFINITION.ID
              .eq(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID)
              .and(finalScopeConditional),
          ).where(*conditions)
          .fetch()
      },
    )
  }

  @Throws(IOException::class)
  private fun getOrganizationIdFromWorkspaceId(scopeId: UUID?): Optional<UUID> {
    val optionalRecord =
      database.query<Optional<Record1<UUID?>?>>(
        { ctx: DSLContext? ->
          ctx!!
            .select(Tables.WORKSPACE.ORGANIZATION_ID)
            .from(
              Tables.WORKSPACE,
            ).where(Tables.WORKSPACE.ID.eq(scopeId))
            .fetchOptional()
        },
      )
    return optionalRecord.map { obj: Record1<UUID?>? -> obj!!.value1() }
  }

  private fun <T> actorDefinitionWithGrantStatus(
    outerJoinRecord: Record,
    recordToActorDefinition: Function<Record, T>,
  ): MutableMap.MutableEntry<T, Boolean> {
    val actorDefinition = recordToActorDefinition.apply(outerJoinRecord)
    val granted = outerJoinRecord.get(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID) != null
    return entry(actorDefinition, granted)
  }

  private fun writeSourceConnection(
    configs: MutableList<SourceConnection?>,
    ctx: DSLContext,
  ) {
    val timestamp = OffsetDateTime.now()
    configs.forEach(
      Consumer { sourceConnection: SourceConnection? ->
        val isExistingConfig =
          ctx.fetchExists(
            DSL
              .select()
              .from(Tables.ACTOR)
              .where(Tables.ACTOR.ID.eq(sourceConnection!!.getSourceId())),
          )
        if (isExistingConfig) {
          ctx
            .update(Tables.ACTOR)
            .set(Tables.ACTOR.ID, sourceConnection.getSourceId())
            .set(Tables.ACTOR.WORKSPACE_ID, sourceConnection.getWorkspaceId())
            .set(Tables.ACTOR.ACTOR_DEFINITION_ID, sourceConnection.getSourceDefinitionId())
            .set(Tables.ACTOR.NAME, sourceConnection.getName())
            .set(Tables.ACTOR.CONFIGURATION, JSONB.valueOf(Jsons.serialize(sourceConnection.getConfiguration())))
            .set(Tables.ACTOR.ACTOR_TYPE, ActorType.source)
            .set(Tables.ACTOR.TOMBSTONE, sourceConnection.getTombstone() != null && sourceConnection.getTombstone())
            .set(Tables.ACTOR.UPDATED_AT, timestamp)
            .set(
              Tables.ACTOR.RESOURCE_REQUIREMENTS,
              JSONB.valueOf(Jsons.serialize(sourceConnection.getResourceRequirements())),
            ).where(Tables.ACTOR.ID.eq(sourceConnection.getSourceId()))
            .execute()
        } else {
          ctx
            .insertInto(Tables.ACTOR)
            .set(Tables.ACTOR.ID, sourceConnection.getSourceId())
            .set(Tables.ACTOR.WORKSPACE_ID, sourceConnection.getWorkspaceId())
            .set(Tables.ACTOR.ACTOR_DEFINITION_ID, sourceConnection.getSourceDefinitionId())
            .set(Tables.ACTOR.NAME, sourceConnection.getName())
            .set(Tables.ACTOR.CONFIGURATION, JSONB.valueOf(Jsons.serialize(sourceConnection.getConfiguration())))
            .set(Tables.ACTOR.ACTOR_TYPE, ActorType.source)
            .set(Tables.ACTOR.TOMBSTONE, sourceConnection.getTombstone() != null && sourceConnection.getTombstone())
            .set(Tables.ACTOR.CREATED_AT, timestamp)
            .set(Tables.ACTOR.UPDATED_AT, timestamp)
            .set(
              Tables.ACTOR.RESOURCE_REQUIREMENTS,
              JSONB.valueOf(Jsons.serialize(sourceConnection.getResourceRequirements())),
            ).execute()
        }
      },
    )
  }

  @Throws(IOException::class)
  private fun listSourceQuery(configId: Optional<UUID>): Stream<SourceConnection> {
    val result =
      database.query<Result<Record>>(
        ContextQueryFunction { ctx: DSLContext? ->
          val query = ctx!!.select(DSL.asterisk()).from(Tables.ACTOR)
          if (configId.isPresent) {
            return@ContextQueryFunction query.where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.source), Tables.ACTOR.ID.eq(configId.get())).fetch()
          }
          return@ContextQueryFunction query.where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.source)).fetch()
        },
      )

    return result.map { record: Record -> DbConverter.buildSourceConnection(record) }.stream()
  }

  private fun includeTombstones(
    tombstoneField: Field<Boolean?>,
    includeTombstones: Boolean,
  ): Condition {
    if (includeTombstones) {
      return DSL.trueCondition()
    } else {
      return tombstoneField.eq(false)
    }
  }

  /**
   * Delete source: tombstone source and void config. Assumes airbyte-managed secrets were already
   * deleted by the domain layer.
   *
   * @param name Source name
   * @param workspaceId workspace ID
   * @param sourceId source ID
   * @throws JsonValidationException if the config is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(ConfigNotFoundException::class, JsonValidationException::class, IOException::class)
  override fun tombstoneSource(
    name: String,
    workspaceId: UUID,
    sourceId: UUID,
  ) {
    val sourceConnection = getSourceConnection(sourceId)

    // Tombstone source and void config
    val newSourceConnection =
      SourceConnection()
        .withName(name)
        .withSourceDefinitionId(sourceConnection.getSourceDefinitionId())
        .withWorkspaceId(workspaceId)
        .withSourceId(sourceId)
        .withConfiguration(null)
        .withTombstone(true)
    writeSourceConnectionNoSecrets(newSourceConnection)
  }

  override fun getSourceConnectionIfExists(sourceId: UUID): Optional<SourceConnection> {
    try {
      return Optional.of(getSourceConnection(sourceId))
    } catch (_: Exception) {
      log.warn("Unable to find source with ID $sourceId")
      return Optional.empty()
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}

    private fun writeStandardSourceDefinition(
      configs: MutableList<StandardSourceDefinition?>,
      ctx: DSLContext,
    ) {
      val timestamp = OffsetDateTime.now()
      configs.forEach(
        Consumer { standardSourceDefinition: StandardSourceDefinition? ->
          val isExistingConfig =
            ctx.fetchExists(
              DSL
                .select()
                .from(Tables.ACTOR_DEFINITION)
                .where(Tables.ACTOR_DEFINITION.ID.eq(standardSourceDefinition!!.getSourceDefinitionId())),
            )
          if (isExistingConfig) {
            ctx
              .update(Tables.ACTOR_DEFINITION)
              .set(Tables.ACTOR_DEFINITION.ID, standardSourceDefinition.getSourceDefinitionId())
              .set(Tables.ACTOR_DEFINITION.NAME, standardSourceDefinition.getName())
              .set(Tables.ACTOR_DEFINITION.ICON, standardSourceDefinition.getIcon())
              .set(Tables.ACTOR_DEFINITION.ICON_URL, standardSourceDefinition.getIconUrl())
              .set(Tables.ACTOR_DEFINITION.ACTOR_TYPE, ActorType.source)
              .set(
                Tables.ACTOR_DEFINITION.SOURCE_TYPE,
                if (standardSourceDefinition.getSourceType() == null) {
                  null
                } else {
                  standardSourceDefinition.getSourceType().value().toEnum<SourceType>()!!
                },
              ).set(Tables.ACTOR_DEFINITION.TOMBSTONE, standardSourceDefinition.getTombstone())
              .set(Tables.ACTOR_DEFINITION.PUBLIC, standardSourceDefinition.getPublic())
              .set(Tables.ACTOR_DEFINITION.CUSTOM, standardSourceDefinition.getCustom())
              .set(Tables.ACTOR_DEFINITION.ENTERPRISE, standardSourceDefinition.getEnterprise())
              .set(
                Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS,
                if (standardSourceDefinition.getResourceRequirements() == null) {
                  null
                } else {
                  JSONB.valueOf(Jsons.serialize(standardSourceDefinition.getResourceRequirements()))
                },
              ).set(Tables.ACTOR_DEFINITION.UPDATED_AT, timestamp)
              .set(
                Tables.ACTOR_DEFINITION.MAX_SECONDS_BETWEEN_MESSAGES,
                if (standardSourceDefinition.getMaxSecondsBetweenMessages() == null) {
                  null
                } else {
                  standardSourceDefinition.getMaxSecondsBetweenMessages().toInt()
                },
              ).set(
                Tables.ACTOR_DEFINITION.METRICS,
                if (standardSourceDefinition.getMetrics() == null) {
                  null
                } else {
                  JSONB.valueOf(Jsons.serialize(standardSourceDefinition.getMetrics()))
                },
              ).where(Tables.ACTOR_DEFINITION.ID.eq(standardSourceDefinition.getSourceDefinitionId()))
              .execute()
          } else {
            ctx
              .insertInto(Tables.ACTOR_DEFINITION)
              .set(Tables.ACTOR_DEFINITION.ID, standardSourceDefinition.getSourceDefinitionId())
              .set(Tables.ACTOR_DEFINITION.NAME, standardSourceDefinition.getName())
              .set(Tables.ACTOR_DEFINITION.ICON, standardSourceDefinition.getIcon())
              .set(Tables.ACTOR_DEFINITION.ICON_URL, standardSourceDefinition.getIconUrl())
              .set(Tables.ACTOR_DEFINITION.ACTOR_TYPE, ActorType.source)
              .set(
                Tables.ACTOR_DEFINITION.SOURCE_TYPE,
                if (standardSourceDefinition.getSourceType() == null) {
                  null
                } else {
                  standardSourceDefinition.getSourceType().value().toEnum<SourceType>()!!
                },
              ).set(
                Tables.ACTOR_DEFINITION.TOMBSTONE,
                standardSourceDefinition.getTombstone() != null && standardSourceDefinition.getTombstone(),
              ).set(Tables.ACTOR_DEFINITION.PUBLIC, standardSourceDefinition.getPublic())
              .set(Tables.ACTOR_DEFINITION.CUSTOM, standardSourceDefinition.getCustom())
              .set(Tables.ACTOR_DEFINITION.ENTERPRISE, standardSourceDefinition.getEnterprise())
              .set(
                Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS,
                if (standardSourceDefinition.getResourceRequirements() == null) {
                  null
                } else {
                  JSONB.valueOf(Jsons.serialize(standardSourceDefinition.getResourceRequirements()))
                },
              ).set(Tables.ACTOR_DEFINITION.CREATED_AT, timestamp)
              .set(Tables.ACTOR_DEFINITION.UPDATED_AT, timestamp)
              .set(
                Tables.ACTOR_DEFINITION.MAX_SECONDS_BETWEEN_MESSAGES,
                if (standardSourceDefinition.getMaxSecondsBetweenMessages() == null) {
                  null
                } else {
                  standardSourceDefinition.getMaxSecondsBetweenMessages().toInt()
                },
              ).set(
                Tables.ACTOR_DEFINITION.METRICS,
                if (standardSourceDefinition.getMetrics() == null) {
                  null
                } else {
                  JSONB.valueOf(Jsons.serialize(standardSourceDefinition.getMetrics()))
                },
              ).execute()
          }
        },
      )
    }
  }
}
