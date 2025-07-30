/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.DestinationConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.data.services.shared.DestinationAndDefinition
import io.airbyte.data.services.shared.DestinationConnectionWithCount
import io.airbyte.data.services.shared.Filters
import io.airbyte.data.services.shared.ResourcesQueryPaginated
import io.airbyte.data.services.shared.SortKey
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType
import io.airbyte.featureflag.FeatureFlagClient
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
class DestinationServiceJooqImpl
  @VisibleForTesting
  constructor(
    @Named("configDatabase") database: Database,
    featureFlagClient: FeatureFlagClient,
    connectionService: ConnectionService,
    actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater,
    metricClient: MetricClient,
    actorPaginationServiceHelper: ActorServicePaginationHelper,
  ) : DestinationService {
    private val database: ExceptionWrappingDatabase
    private val featureFlagClient: FeatureFlagClient
    private val connectionService: ConnectionService
    private val actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater
    private val metricClient: MetricClient
    private val actorPaginationServiceHelper: ActorServicePaginationHelper

    init {
      this.database = ExceptionWrappingDatabase(database)
      this.connectionService = connectionService
      this.featureFlagClient = featureFlagClient
      this.actorDefinitionVersionUpdater = actorDefinitionVersionUpdater
      this.metricClient = metricClient
      this.actorPaginationServiceHelper = actorPaginationServiceHelper
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    override fun getStandardDestinationDefinition(destinationDefinitionId: UUID): StandardDestinationDefinition =
      getStandardDestinationDefinition(destinationDefinitionId, true)

    /**
     * Get destination definition.
     *
     * @param destinationDefinitionId destination definition id
     * @return destination definition
     * @throws JsonValidationException - throws if returned sources are invalid
     * @throws IOException - you never know when you IO
     * @throws ConfigNotFoundException - throws if no source with that id can be found.
     */
    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    override fun getStandardDestinationDefinition(
      destinationDefinitionId: UUID,
      includeTombstone: Boolean,
    ): StandardDestinationDefinition =
      destDefQuery(Optional.of(destinationDefinitionId), includeTombstone)
        .findFirst()
        .orElseThrow {
          ConfigNotFoundException(
            ConfigNotFoundType.STANDARD_DESTINATION_DEFINITION,
            destinationDefinitionId,
          )
        }

    /**
     * Get destination definition form destination.
     *
     * @param destinationId destination id
     * @return destination definition
     */
    override fun getDestinationDefinitionFromDestination(destinationId: UUID): StandardDestinationDefinition {
      try {
        val destination = getDestinationConnection(destinationId)
        return getStandardDestinationDefinition(destination.getDestinationDefinitionId())
      } catch (e: Exception) {
        throw RuntimeException(e)
      }
    }

    /**
     * Returns if a destination is active, i.e. the destination has at least one active or manual
     * connection.
     *
     * @param destinationId - id of the destination
     * @return boolean - if destination is active or not
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun isDestinationActive(destinationId: UUID): Boolean =
      database.query(
        { ctx: DSLContext ->
          ctx.fetchExists(
            DSL
              .select()
              .from(Tables.CONNECTION)
              .where(Tables.CONNECTION.DESTINATION_ID.eq(destinationId))
              .and(Tables.CONNECTION.STATUS.eq(StatusType.active)),
          )
        },
      )

    /**
     * Get destination definition used by a connection.
     *
     * @param connectionId connection id
     * @return destination definition
     */
    override fun getDestinationDefinitionFromConnection(connectionId: UUID): StandardDestinationDefinition {
      try {
        val sync = connectionService.getStandardSync(connectionId)
        return getDestinationDefinitionFromDestination(sync.getDestinationId())
      } catch (e: Exception) {
        throw RuntimeException(e)
      }
    }

    /**
     * List standard destination definitions.
     *
     * @param includeTombstone include tombstoned destinations
     * @return list destination definitions
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun listStandardDestinationDefinitions(includeTombstone: Boolean): MutableList<StandardDestinationDefinition> =
      destDefQuery(Optional.empty(), includeTombstone).toList()

    /**
     * List public destination definitions.
     *
     * @param includeTombstone include tombstoned destinations
     * @return public destination definitions
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun listPublicDestinationDefinitions(includeTombstone: Boolean): MutableList<StandardDestinationDefinition> =
      listStandardActorDefinitions(
        ActorType.destination,
        { record: Record -> DbConverter.buildStandardDestinationDefinition(record) },
        includeTombstones(Tables.ACTOR_DEFINITION.TOMBSTONE, includeTombstone),
        Tables.ACTOR_DEFINITION.PUBLIC.eq(true),
      )

    /**
     * List destination definitions used by the given workspace.
     *
     * @param workspaceId workspace id
     * @param includeTombstone include tombstoned destinations
     * @return public destination definitions
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun listDestinationDefinitionsForWorkspace(
      workspaceId: UUID,
      includeTombstone: Boolean,
    ): List<StandardDestinationDefinition> =
      database
        .query<Result<Record>> { ctx: DSLContext ->
          ctx
            .selectDistinct(Tables.ACTOR_DEFINITION.asterisk())
            .from(Tables.ACTOR_DEFINITION)
            .join(Tables.ACTOR)
            .on(Tables.ACTOR.ACTOR_DEFINITION_ID.eq(Tables.ACTOR_DEFINITION.ID))
            .where(Tables.ACTOR.WORKSPACE_ID.eq(workspaceId))
            .and(Tables.ACTOR_DEFINITION.ACTOR_TYPE.eq(ActorType.destination))
            .and(if (includeTombstone) DSL.noCondition() else Tables.ACTOR.TOMBSTONE.notEqual(true))
            .and(if (includeTombstone) DSL.noCondition() else Tables.ACTOR_DEFINITION.TOMBSTONE.notEqual(true))
            .fetch()
        }.stream()
        .map { record: Record -> DbConverter.buildStandardDestinationDefinition(record) }
        .toList()

    /**
     * List granted destination definitions for workspace.
     *
     * @param workspaceId workspace id
     * @param includeTombstones include tombstoned destinations
     * @return list standard destination definitions
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun listGrantedDestinationDefinitions(
      workspaceId: UUID,
      includeTombstones: Boolean,
    ): MutableList<StandardDestinationDefinition> =
      listActorDefinitionsJoinedWithGrants(
        workspaceId,
        ScopeType.workspace,
        JoinType.JOIN,
        ActorType.destination,
        { record: Record -> DbConverter.buildStandardDestinationDefinition(record) },
        includeTombstones(Tables.ACTOR_DEFINITION.TOMBSTONE, includeTombstones),
      )

    /**
     * List destinations to which we can give a grant.
     *
     * @param workspaceId workspace id
     * @param includeTombstones include tombstoned definitions
     * @return list of pairs from destination definition and whether it can be granted
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun listGrantableDestinationDefinitions(
      workspaceId: UUID,
      includeTombstones: Boolean,
    ): MutableList<MutableMap.MutableEntry<StandardDestinationDefinition, Boolean>> =
      listActorDefinitionsJoinedWithGrants(
        workspaceId,
        ScopeType.workspace,
        JoinType.LEFT_OUTER_JOIN,
        ActorType.destination,
        { record: Record -> this.mapRecordToDestinationDefinitionWithGrantStatus(record) },
        Tables.ACTOR_DEFINITION.CUSTOM.eq(false),
        includeTombstones(Tables.ACTOR_DEFINITION.TOMBSTONE, includeTombstones),
      )

    private fun mapRecordToDestinationDefinitionWithGrantStatus(record: Record): MutableMap.MutableEntry<StandardDestinationDefinition, Boolean> =
      actorDefinitionWithGrantStatus(
        record,
        { record: Record? ->
          this.buildDestinationDefinition(
            record!!,
          )
        },
      )

    private fun buildDestinationDefinition(record: Record): StandardDestinationDefinition = DbConverter.buildStandardDestinationDefinition(record)

    /**
     * Update destination definition.
     *
     * @param destinationDefinition destination definition
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    override fun updateStandardDestinationDefinition(destinationDefinition: StandardDestinationDefinition) {
      // Check existence before updating
      // TODO: split out write and update methods so that we don't need explicit checking
      getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId())

      database.transaction<Any?>(
        { ctx: DSLContext ->
          writeStandardDestinationDefinition(mutableListOf(destinationDefinition), ctx)
          null
        },
      )
    }

    /**
     * Returns destination with a given id. Does not contain secrets.
     *
     * @param destinationId - id of destination to fetch.
     * @return destinations
     * @throws JsonValidationException - throws if returned destinations are invalid
     * @throws IOException - you never know when you IO
     * @throws ConfigNotFoundException - throws if no destination with that id can be found.
     */
    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    override fun getDestinationConnection(destinationId: UUID): DestinationConnection =
      listDestinationQuery(Optional.of(destinationId))
        .findFirst()
        .orElseThrow { ConfigNotFoundException(ConfigNotFoundType.DESTINATION_CONNECTION, destinationId) }

    /**
     * MUST NOT ACCEPT SECRETS - Should only be called from { @link SecretsRepositoryWriter }
     *
     *
     * Write a DestinationConnection to the database. The configuration of the Destination will be a
     * partial configuration (no secrets, just pointer to the secrets store).
     *
     * @param partialDestination - The configuration of the Destination will be a partial configuration
     * (no secrets, just pointer to the secrets store)
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun writeDestinationConnectionNoSecrets(partialDestination: DestinationConnection) {
      database.transaction<Any?>(
        { ctx: DSLContext ->
          writeDestinationConnection(mutableListOf(partialDestination), ctx)
          null
        },
      )
    }

    /**
     * Returns all destinations in the database. Does not contain secrets.
     *
     * @return destinations
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun listDestinationConnection(): MutableList<DestinationConnection> = listDestinationQuery(Optional.empty()).toList()

    /**
     * Returns all destinations for a workspace. Does not contain secrets.
     *
     * @param workspaceId - id of the workspace
     * @return destinations
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun listWorkspaceDestinationConnection(workspaceId: UUID): MutableList<DestinationConnection> {
      val result =
        database.query<Result<Record>>(
          { ctx: DSLContext ->
            ctx
              .select(DSL.asterisk())
              .from(Tables.ACTOR)
              .where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.destination))
              .and(Tables.ACTOR.WORKSPACE_ID.eq(workspaceId))
              .andNot(Tables.ACTOR.TOMBSTONE)
              .fetch()
          },
        )
      return result
        .stream()
        .map { record: Record -> DbConverter.buildDestinationConnection(record) }
        .collect(Collectors.toList())
    }

    /**
     * Returns all destinations for a list of workspaces. Does not contain secrets.
     *
     * @param resourcesQueryPaginated - Includes all the things we might want to query
     * @return destinations
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun listWorkspacesDestinationConnections(resourcesQueryPaginated: ResourcesQueryPaginated): MutableList<DestinationConnection> {
      val result =
        database.query<Result<Record>>(
          { ctx: DSLContext ->
            ctx
              .select(DSL.asterisk())
              .from(Tables.ACTOR)
              .where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.destination))
              .and(Tables.ACTOR.WORKSPACE_ID.`in`(resourcesQueryPaginated.workspaceIds))
              .and(if (resourcesQueryPaginated.includeDeleted) DSL.noCondition() else Tables.ACTOR.TOMBSTONE.notEqual(true))
              .limit(resourcesQueryPaginated.pageSize)
              .offset(resourcesQueryPaginated.rowOffset)
              .fetch()
          },
        )
      return result
        .stream()
        .map { record: Record -> DbConverter.buildDestinationConnection(record) }
        .collect(Collectors.toList())
    }

    /**
     * Returns all active destinations using a definition.
     *
     * @param definitionId - id for the definition
     * @return destinations
     * @throws IOException - exception while interacting with the db
     */
    @Throws(IOException::class)
    override fun listDestinationsForDefinition(definitionId: UUID): MutableList<DestinationConnection> {
      val result =
        database.query<Result<Record>>(
          { ctx: DSLContext ->
            ctx
              .select(DSL.asterisk())
              .from(Tables.ACTOR)
              .where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.destination))
              .and(Tables.ACTOR.ACTOR_DEFINITION_ID.eq(definitionId))
              .andNot(Tables.ACTOR.TOMBSTONE)
              .fetch()
          },
        )
      return result
        .stream()
        .map { record: Record -> DbConverter.buildDestinationConnection(record) }
        .collect(Collectors.toList())
    }

    /**
     * Get destination and definition from destinations ids.
     *
     * @param destinationIds destination ids
     * @return pair of destination and definition
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun getDestinationAndDefinitionsFromDestinationIds(destinationIds: List<UUID>): List<DestinationAndDefinition> {
      val records: Result<Record> =
        database.query<Result<Record>>(
          { ctx: DSLContext ->
            ctx
              .select(Tables.ACTOR.asterisk(), Tables.ACTOR_DEFINITION.asterisk())
              .from(Tables.ACTOR)
              .join(Tables.ACTOR_DEFINITION)
              .on(Tables.ACTOR.ACTOR_DEFINITION_ID.eq(Tables.ACTOR_DEFINITION.ID))
              .where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.destination), Tables.ACTOR.ID.`in`(destinationIds))
              .fetch()
          },
        )

      val destinationAndDefinitions: MutableList<DestinationAndDefinition> = ArrayList()

      for (record in records) {
        val destination = DbConverter.buildDestinationConnection(record)
        val definition = DbConverter.buildStandardDestinationDefinition(record)
        destinationAndDefinitions.add(DestinationAndDefinition(destination, definition))
      }

      return destinationAndDefinitions.toList()
    }

    /**
     * Write metadata for a custom destination: global metadata (destination definition) and versioned
     * metadata (actor definition version for the version to use).
     *
     * @param destinationDefinition destination definition
     * @param defaultVersion default actor definition version
     * @param scopeId workspace or organization id
     * @param scopeType enum of workspace or organization
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun writeCustomConnectorMetadata(
      destinationDefinition: StandardDestinationDefinition,
      defaultVersion: ActorDefinitionVersion,
      scopeId: UUID,
      scopeType: io.airbyte.config.ScopeType,
    ) {
      database.transaction<Any?>(
        { ctx: DSLContext ->
          writeConnectorMetadata(destinationDefinition, defaultVersion, listOf(), ctx)
          writeActorDefinitionWorkspaceGrant(
            destinationDefinition.getDestinationDefinitionId(),
            scopeId,
            ScopeType.valueOf(scopeType.toString()),
            ctx,
          )
          null
        },
      )

      actorDefinitionVersionUpdater.updateDestinationDefaultVersion(
        destinationDefinition,
        defaultVersion,
        mutableListOf(),
      )
    }

    /**
     * Write metadata for a destination connector. Writes global metadata (destination definition) and
     * versioned metadata (info for actor definition version to set as default). Sets the new version as
     * the default version and updates actors accordingly, based on whether the upgrade will be breaking
     * or not.
     *
     * @param destinationDefinition standard destination definition
     * @param actorDefinitionVersion actor definition version
     * @param breakingChangesForDefinition - list of breaking changes for the definition
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun writeConnectorMetadata(
      destinationDefinition: StandardDestinationDefinition,
      actorDefinitionVersion: ActorDefinitionVersion,
      breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
    ) {
      database.transaction<Any?>(
        { ctx: DSLContext ->
          writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, breakingChangesForDefinition, ctx)
          null
        },
      )

      // FIXME(pedro): this should be moved out of this service
      actorDefinitionVersionUpdater.updateDestinationDefaultVersion(destinationDefinition, actorDefinitionVersion, breakingChangesForDefinition)
    }

    @Throws(IOException::class)
    override fun listDestinationsWithIds(destinationIds: List<UUID>): List<DestinationConnection> {
      val result =
        database.query<Result<Record>>(
          { ctx: DSLContext ->
            ctx
              .select(DSL.asterisk())
              .from(Tables.ACTOR)
              .where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.destination))
              .and(Tables.ACTOR.ID.`in`(destinationIds))
              .andNot(Tables.ACTOR.TOMBSTONE)
              .fetch()
          },
        )
      return result.stream().map { record: Record -> DbConverter.buildDestinationConnection(record) }.toList()
    }

    private fun writeActorDefinitionWorkspaceGrant(
      actorDefinitionId: UUID?,
      scopeId: UUID?,
      scopeType: ScopeType?,
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
      destinationDefinition: StandardDestinationDefinition?,
      actorDefinitionVersion: ActorDefinitionVersion,
      breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
      ctx: DSLContext,
    ) {
      writeStandardDestinationDefinition(mutableListOf(destinationDefinition), ctx)
      ConnectorMetadataJooqHelper.writeActorDefinitionBreakingChanges(breakingChangesForDefinition, ctx)
      ConnectorMetadataJooqHelper.writeActorDefinitionVersion(actorDefinitionVersion, ctx)
    }

    @Throws(IOException::class)
    private fun destDefQuery(
      destDefId: Optional<UUID>,
      includeTombstone: Boolean,
    ): Stream<StandardDestinationDefinition> =
      database
        .query<Result<Record>> { ctx: DSLContext ->
          ctx
            .select(Tables.ACTOR_DEFINITION.asterisk())
            .from(Tables.ACTOR_DEFINITION)
            .where(Tables.ACTOR_DEFINITION.ACTOR_TYPE.eq(ActorType.destination))
            .and(destDefId.map { t: UUID? -> Tables.ACTOR_DEFINITION.ID.eq(t) }.orElse(DSL.noCondition()))
            .and(if (includeTombstone) DSL.noCondition() else Tables.ACTOR_DEFINITION.TOMBSTONE.notEqual(true))
            .fetch()
        }.stream()
        .map { record: Record -> DbConverter.buildStandardDestinationDefinition(record) }

    @Throws(IOException::class)
    private fun <T> listStandardActorDefinitions(
      actorType: ActorType,
      recordToActorDefinition: Function<Record, T>,
      vararg conditions: Condition?,
    ): MutableList<T> {
      val records =
        database.query<Result<Record>>(
          { ctx: DSLContext ->
            ctx
              .select(DSL.asterisk())
              .from(Tables.ACTOR_DEFINITION)
              .where(*conditions)
              .and(Tables.ACTOR_DEFINITION.ACTOR_TYPE.eq(actorType))
              .fetch()
          },
        )

      return records
        .stream()
        .map(recordToActorDefinition)
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
        { ctx: DSLContext ->
          ctx
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
        database.query<Optional<Record1<UUID?>>>(
          { ctx: DSLContext ->
            ctx
              .select(Tables.WORKSPACE.ORGANIZATION_ID)
              .from(
                Tables.WORKSPACE,
              ).where(Tables.WORKSPACE.ID.eq(scopeId))
              .fetchOptional()
          },
        )
      return optionalRecord.map({ obj: Record1<UUID?>? -> obj!!.value1() })
    }

    private fun writeDestinationConnection(
      configs: MutableList<DestinationConnection?>,
      ctx: DSLContext,
    ) {
      val timestamp = OffsetDateTime.now()
      configs.forEach(
        Consumer { destinationConnection: DestinationConnection? ->
          val isExistingConfig =
            ctx.fetchExists(
              DSL
                .select()
                .from(Tables.ACTOR)
                .where(Tables.ACTOR.ID.eq(destinationConnection!!.destinationId)),
            )
          if (isExistingConfig) {
            ctx
              .update(Tables.ACTOR)
              .set(Tables.ACTOR.ID, destinationConnection.destinationId)
              .set(Tables.ACTOR.WORKSPACE_ID, destinationConnection.workspaceId)
              .set(Tables.ACTOR.ACTOR_DEFINITION_ID, destinationConnection.destinationDefinitionId)
              .set(Tables.ACTOR.NAME, destinationConnection.name)
              .set(Tables.ACTOR.CONFIGURATION, JSONB.valueOf(Jsons.serialize(destinationConnection.configuration)))
              .set(Tables.ACTOR.ACTOR_TYPE, ActorType.destination)
              .set(Tables.ACTOR.TOMBSTONE, destinationConnection.tombstone != null && destinationConnection.tombstone)
              .set(Tables.ACTOR.UPDATED_AT, timestamp)
              .set(
                Tables.ACTOR.RESOURCE_REQUIREMENTS,
                JSONB.valueOf(Jsons.serialize(destinationConnection.resourceRequirements)),
              ).where(Tables.ACTOR.ID.eq(destinationConnection.destinationId))
              .execute()
          } else {
            ctx
              .insertInto(Tables.ACTOR)
              .set(Tables.ACTOR.ID, destinationConnection.destinationId)
              .set(Tables.ACTOR.WORKSPACE_ID, destinationConnection.workspaceId)
              .set(Tables.ACTOR.ACTOR_DEFINITION_ID, destinationConnection.destinationDefinitionId)
              .set(Tables.ACTOR.NAME, destinationConnection.name)
              .set(Tables.ACTOR.CONFIGURATION, JSONB.valueOf(Jsons.serialize(destinationConnection.configuration)))
              .set(Tables.ACTOR.ACTOR_TYPE, ActorType.destination)
              .set(Tables.ACTOR.TOMBSTONE, destinationConnection.tombstone != null && destinationConnection.tombstone)
              .set(Tables.ACTOR.CREATED_AT, timestamp)
              .set(Tables.ACTOR.UPDATED_AT, timestamp)
              .set(
                Tables.ACTOR.RESOURCE_REQUIREMENTS,
                JSONB.valueOf(Jsons.serialize(destinationConnection.resourceRequirements)),
              ).execute()
          }
        },
      )
    }

    private fun includeTombstones(
      tombstoneField: Field<Boolean?>,
      includeTombstones: Boolean,
    ): Condition =
      if (includeTombstones) {
        DSL.trueCondition()
      } else {
        tombstoneField.eq(false)
      }

    private fun <T> actorDefinitionWithGrantStatus(
      outerJoinRecord: Record,
      recordToActorDefinition: Function<Record, T>,
    ): MutableMap.MutableEntry<T, Boolean> {
      val actorDefinition = recordToActorDefinition.apply(outerJoinRecord)
      val granted = outerJoinRecord.get(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID) != null
      return entry(actorDefinition, granted)
    }

    @Throws(IOException::class)
    private fun listDestinationQuery(configId: Optional<UUID>): Stream<DestinationConnection> {
      val result =
        database.query<Result<Record>>(
          ContextQueryFunction { ctx: DSLContext ->
            val query = ctx.select(DSL.asterisk()).from(Tables.ACTOR)
            if (configId.isPresent) {
              return@ContextQueryFunction query.where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.destination), Tables.ACTOR.ID.eq(configId.get())).fetch()
            }
            return@ContextQueryFunction query.where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.destination)).fetch()
          },
        )

      return result.map { record: Record -> DbConverter.buildDestinationConnection(record) }.stream()
    }

    /**
     * Delete destination: tombstone destination. Assumes secrets have already been deleted by the
     * domain layer.
     *
     * @param name Destination name
     * @param workspaceId workspace ID
     * @param destinationId destination ID
     * @param spec spec for the destination
     * @throws JsonValidationException if the config is or contains invalid json
     * @throws IOException if there is an issue while interacting with the secrets store or db.
     */
    @Throws(ConfigNotFoundException::class, JsonValidationException::class, IOException::class)
    override fun tombstoneDestination(
      name: String,
      workspaceId: UUID,
      destinationId: UUID,
    ) {
      val destinationConnection = getDestinationConnection(destinationId)

      // Tombstone destination and void config
      val newDestinationConnection =
        DestinationConnection()
          .withName(name)
          .withDestinationDefinitionId(destinationConnection.getDestinationDefinitionId())
          .withWorkspaceId(workspaceId)
          .withDestinationId(destinationId)
          .withConfiguration(null)
          .withTombstone(true)
      writeDestinationConnectionNoSecrets(newDestinationConnection)
    }

    override fun getDestinationConnectionIfExists(destinationId: UUID): Optional<DestinationConnection> {
      try {
        return Optional.of(getDestinationConnection(destinationId))
      } catch (_: Exception) {
        log.warn("Unable to find destination with ID $destinationId")
        return Optional.empty()
      }
    }

    override fun listWorkspaceDestinationConnectionsWithCounts(
      workspaceId: UUID,
      workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
    ): List<DestinationConnectionWithCount> =
      actorPaginationServiceHelper
        .listWorkspaceActorConnectionsWithCounts(
          workspaceId,
          workspaceResourceCursorPagination,
          ActorType.destination,
        ).map { actorConnectionWithCount ->
          actorConnectionWithCount.destinationConnection?.let { destination ->
            DestinationConnectionWithCount(
              destination,
              actorConnectionWithCount.actorDefinitionName,
              actorConnectionWithCount.connectionCount,
              actorConnectionWithCount.lastSync,
              actorConnectionWithCount.connectionJobStatuses,
              actorConnectionWithCount.isActive,
            )
          } ?: throw IllegalStateException("Expected destination connection for destination actor type")
        }

    @Throws(IOException::class)
    override fun countWorkspaceDestinationsFiltered(
      workspaceId: UUID,
      workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
    ): Int = actorPaginationServiceHelper.countWorkspaceActorsFiltered(workspaceId, workspaceResourceCursorPagination, ActorType.destination)

    override fun buildCursorPagination(
      cursor: UUID?,
      internalSortKey: SortKey,
      filters: Filters?,
      ascending: Boolean?,
      pageSize: Int?,
    ): WorkspaceResourceCursorPagination? =
      actorPaginationServiceHelper.buildCursorPagination(cursor, internalSortKey, filters, ascending, pageSize, ActorType.destination)

    companion object {
      private val log = KotlinLogging.logger {}

      fun writeStandardDestinationDefinition(
        configs: MutableList<StandardDestinationDefinition?>,
        ctx: DSLContext,
      ) {
        val timestamp = OffsetDateTime.now()
        configs.forEach(
          Consumer { standardDestinationDefinition: StandardDestinationDefinition? ->
            val isExistingConfig =
              ctx.fetchExists(
                DSL
                  .select()
                  .from(Tables.ACTOR_DEFINITION)
                  .where(Tables.ACTOR_DEFINITION.ID.eq(standardDestinationDefinition!!.getDestinationDefinitionId())),
              )
            if (isExistingConfig) {
              ctx
                .update(Tables.ACTOR_DEFINITION)
                .set(Tables.ACTOR_DEFINITION.ID, standardDestinationDefinition.getDestinationDefinitionId())
                .set(Tables.ACTOR_DEFINITION.NAME, standardDestinationDefinition.getName())
                .set(Tables.ACTOR_DEFINITION.ICON, standardDestinationDefinition.getIcon())
                .set(Tables.ACTOR_DEFINITION.ICON_URL, standardDestinationDefinition.getIconUrl())
                .set(Tables.ACTOR_DEFINITION.ACTOR_TYPE, ActorType.destination)
                .set(Tables.ACTOR_DEFINITION.TOMBSTONE, standardDestinationDefinition.getTombstone())
                .set(Tables.ACTOR_DEFINITION.PUBLIC, standardDestinationDefinition.getPublic())
                .set(Tables.ACTOR_DEFINITION.CUSTOM, standardDestinationDefinition.getCustom())
                .set(Tables.ACTOR_DEFINITION.ENTERPRISE, standardDestinationDefinition.getEnterprise())
                .set(
                  Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS,
                  if (standardDestinationDefinition.getResourceRequirements() == null) {
                    null
                  } else {
                    JSONB.valueOf(Jsons.serialize(standardDestinationDefinition.getResourceRequirements()))
                  },
                ).set(
                  Tables.ACTOR_DEFINITION.METRICS,
                  if (standardDestinationDefinition.getMetrics() == null) {
                    null
                  } else {
                    JSONB.valueOf(Jsons.serialize(standardDestinationDefinition.getMetrics()))
                  },
                ).set(Tables.ACTOR_DEFINITION.UPDATED_AT, timestamp)
                .where(Tables.ACTOR_DEFINITION.ID.eq(standardDestinationDefinition.getDestinationDefinitionId()))
                .execute()
            } else {
              ctx
                .insertInto(Tables.ACTOR_DEFINITION)
                .set(Tables.ACTOR_DEFINITION.ID, standardDestinationDefinition.getDestinationDefinitionId())
                .set(Tables.ACTOR_DEFINITION.NAME, standardDestinationDefinition.getName())
                .set(Tables.ACTOR_DEFINITION.ICON, standardDestinationDefinition.getIcon())
                .set(Tables.ACTOR_DEFINITION.ICON_URL, standardDestinationDefinition.getIconUrl())
                .set(Tables.ACTOR_DEFINITION.ACTOR_TYPE, ActorType.destination)
                .set(
                  Tables.ACTOR_DEFINITION.TOMBSTONE,
                  standardDestinationDefinition.getTombstone() != null && standardDestinationDefinition.getTombstone(),
                ).set(Tables.ACTOR_DEFINITION.PUBLIC, standardDestinationDefinition.getPublic())
                .set(Tables.ACTOR_DEFINITION.CUSTOM, standardDestinationDefinition.getCustom())
                .set(Tables.ACTOR_DEFINITION.ENTERPRISE, standardDestinationDefinition.getEnterprise())
                .set(
                  Tables.ACTOR_DEFINITION.RESOURCE_REQUIREMENTS,
                  if (standardDestinationDefinition.getResourceRequirements() == null) {
                    null
                  } else {
                    JSONB.valueOf(Jsons.serialize(standardDestinationDefinition.getResourceRequirements()))
                  },
                ).set(
                  Tables.ACTOR_DEFINITION.METRICS,
                  if (standardDestinationDefinition.getMetrics() == null) {
                    null
                  } else {
                    JSONB.valueOf(Jsons.serialize(standardDestinationDefinition.getMetrics()))
                  },
                ).set(Tables.ACTOR_DEFINITION.CREATED_AT, timestamp)
                .set(Tables.ACTOR_DEFINITION.UPDATED_AT, timestamp)
                .execute()
            }
          },
        )
      }
    }
  }
