/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.Resources
import io.airbyte.commons.yaml.Yamls
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ResourcesQueryPaginated
import io.airbyte.data.services.shared.StandardSyncQuery
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage
import io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.metrics.MetricClient
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.JoinType
import org.jooq.Record
import org.jooq.Record1
import org.jooq.Result
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.stream.Collectors
import java.util.stream.Stream

private val log = KotlinLogging.logger {}

@Singleton
class WorkspaceServiceJooqImpl
  @VisibleForTesting
  constructor(
    @Named("configDatabase") database: Database?,
    private val featureFlagClient: FeatureFlagClient,
    private val secretsRepositoryReader: SecretsRepositoryReader,
    private val secretsRepositoryWriter: SecretsRepositoryWriter,
    private val secretPersistenceConfigService: SecretPersistenceConfigService,
    private val metricClient: MetricClient,
  ) : WorkspaceService {
    private val database = ExceptionWrappingDatabase(database)
    private val connectionService = ConnectionServiceJooqImpl(database)

    /**
     * Get workspace.
     *
     * @param workspaceId workspace id
     * @param includeTombstone include tombestoned workspace
     * @return workspace
     * @throws JsonValidationException - throws if returned sources are invalid
     * @throws IOException - you never know when you IO
     * @throws ConfigNotFoundException - throws if no source with that id can be found.
     */
    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    override fun getStandardWorkspaceNoSecrets(
      workspaceId: UUID,
      includeTombstone: Boolean,
    ): StandardWorkspace =
      listWorkspaceQuery(Optional.of(java.util.List.of(workspaceId)), includeTombstone)
        .findFirst()
        .orElseThrow {
          ConfigNotFoundException(
            ConfigNotFoundType.STANDARD_WORKSPACE,
            workspaceId,
          )
        }

    /**
     * Get workspace from slug.
     *
     * @param slug to use to find the workspace
     * @param includeTombstone include tombestoned workspace
     * @return workspace, if present.
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun getWorkspaceBySlugOptional(
      slug: String,
      includeTombstone: Boolean,
    ): Optional<StandardWorkspace> {
      val result: Result<Record> =
        if (includeTombstone) {
          database
            .query { ctx: DSLContext ->
              ctx
                .select(Tables.WORKSPACE.asterisk())
                .from(Tables.WORKSPACE)
                .where(Tables.WORKSPACE.SLUG.eq(slug))
            }.fetch()
        } else {
          database
            .query { ctx: DSLContext ->
              ctx
                .select(Tables.WORKSPACE.asterisk())
                .from(Tables.WORKSPACE)
                .where(Tables.WORKSPACE.SLUG.eq(slug))
                .andNot(Tables.WORKSPACE.TOMBSTONE)
            }.fetch()
        }

      return result.stream().findFirst().map { record: Record -> DbConverter.buildStandardWorkspace(record) }
    }

    /**
     * Get workspace from slug.
     *
     * @param slug to use to find the workspace
     * @param includeTombstone include tombestoned workspace
     * @return workspace
     * @throws IOException - you never know when you IO
     * @throws ConfigNotFoundException - throws if no source with that id can be found.
     */
    @Throws(IOException::class, ConfigNotFoundException::class)
    override fun getWorkspaceBySlug(
      slug: String,
      includeTombstone: Boolean,
    ): StandardWorkspace =
      getWorkspaceBySlugOptional(slug, includeTombstone)
        .orElseThrow {
          ConfigNotFoundException(
            ConfigNotFoundType.STANDARD_WORKSPACE,
            slug,
          )
        }

    /**
     * List workspaces.
     *
     * @param includeTombstone include tombstoned workspaces
     * @return workspaces
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun listStandardWorkspaces(includeTombstone: Boolean): List<StandardWorkspace> =
      listWorkspaceQuery(Optional.empty(), includeTombstone).toList()

    @Throws(IOException::class)
    override fun listWorkspaceQuery(
      workspaceIds: Optional<List<UUID>>,
      includeTombstone: Boolean,
    ): Stream<StandardWorkspace> =
      database
        .query { ctx: DSLContext ->
          ctx
            .select(Tables.WORKSPACE.asterisk())
            .from(Tables.WORKSPACE)
            .where(
              if (includeTombstone) {
                DSL.noCondition()
              } else {
                Tables.WORKSPACE.TOMBSTONE.notEqual(
                  true,
                )
              },
            ).and(
              workspaceIds
                .map { collection: List<UUID?>? ->
                  Tables.WORKSPACE.ID.`in`(
                    collection,
                  )
                }.orElse(DSL.noCondition()),
            ).fetch()
        }.stream()
        .map { record: Record -> DbConverter.buildStandardWorkspace(record) }

    /**
     * List workspaces (paginated).
     *
     * @param resourcesQueryPaginated - contains all the information we need to paginate
     * @return A List of StandardWorkspace objects
     * @throws IOException you never know when you IO
     */
    @Throws(IOException::class)
    override fun listStandardWorkspacesPaginated(resourcesQueryPaginated: ResourcesQueryPaginated): List<StandardWorkspace> =
      database
        .query { ctx: DSLContext ->
          ctx
            .select(Tables.WORKSPACE.asterisk())
            .from(Tables.WORKSPACE)
            .where(
              if (resourcesQueryPaginated.includeDeleted) {
                DSL.noCondition()
              } else {
                Tables.WORKSPACE.TOMBSTONE.notEqual(
                  true,
                )
              },
            ).and(Tables.WORKSPACE.ID.`in`(resourcesQueryPaginated.workspaceIds))
            .limit(resourcesQueryPaginated.pageSize)
            .offset(resourcesQueryPaginated.rowOffset)
            .fetch()
        }.stream()
        .map { record: Record -> DbConverter.buildStandardWorkspace(record) }
        .toList()

    /**
     * Get workspace for a connection.
     *
     * @param connectionId connection id
     * @param isTombstone include tombstoned workspaces
     * @return workspace to which the connection belongs
     */
    @Throws(ConfigNotFoundException::class)
    override fun getStandardWorkspaceFromConnection(
      connectionId: UUID,
      isTombstone: Boolean,
    ): StandardWorkspace {
      try {
        val sync = connectionService.getStandardSync(connectionId)
        val source = getSourceConnection(sync.sourceId)
        return getStandardWorkspaceNoSecrets(source.workspaceId, isTombstone)
      } catch (e: ConfigNotFoundException) {
        throw e
      } catch (e: Exception) {
        throw RuntimeException(e)
      }
    }

    /**
     * MUST NOT ACCEPT SECRETS - Should only be called from { @link SecretsRepositoryWriter }.
     *
     *
     * Write a StandardWorkspace to the database.
     *
     * @param workspace - The configuration of the workspace
     * @throws JsonValidationException - throws is the workspace is invalid
     * @throws IOException - you never know when you IO
     */
    @Throws(JsonValidationException::class, IOException::class)
    override fun writeStandardWorkspaceNoSecrets(workspace: StandardWorkspace) {
      database.transaction<Any?> { ctx: DSLContext ->
        val timestamp = OffsetDateTime.now()
        val isExistingConfig =
          ctx.fetchExists(
            DSL
              .select()
              .from(Tables.WORKSPACE)
              .where(Tables.WORKSPACE.ID.eq(workspace.workspaceId)),
          )

        if (isExistingConfig) {
          ctx
            .update(Tables.WORKSPACE)
            .set(Tables.WORKSPACE.ID, workspace.workspaceId)
            .set(Tables.WORKSPACE.CUSTOMER_ID, workspace.customerId)
            .set(Tables.WORKSPACE.NAME, workspace.name)
            .set(Tables.WORKSPACE.SLUG, workspace.slug)
            .set(Tables.WORKSPACE.EMAIL, workspace.email)
            .set(
              Tables.WORKSPACE.INITIAL_SETUP_COMPLETE,
              workspace.initialSetupComplete,
            ).set(
              Tables.WORKSPACE.ANONYMOUS_DATA_COLLECTION,
              workspace.anonymousDataCollection,
            ).set(Tables.WORKSPACE.SEND_NEWSLETTER, workspace.news)
            .set(
              Tables.WORKSPACE.SEND_SECURITY_UPDATES,
              workspace.securityUpdates,
            ).set(
              Tables.WORKSPACE.DISPLAY_SETUP_WIZARD,
              workspace.displaySetupWizard,
            ).set(
              Tables.WORKSPACE.TOMBSTONE,
              workspace.tombstone != null && workspace.tombstone,
            ).set(
              Tables.WORKSPACE.NOTIFICATIONS,
              JSONB.valueOf(
                Jsons.serialize(workspace.notifications),
              ),
            ).set(
              Tables.WORKSPACE.NOTIFICATION_SETTINGS,
              JSONB.valueOf(Jsons.serialize(workspace.notificationSettings)),
            ).set(
              Tables.WORKSPACE.FIRST_SYNC_COMPLETE,
              workspace.firstCompletedSync,
            ).set(Tables.WORKSPACE.FEEDBACK_COMPLETE, workspace.feedbackDone)
            .set(
              Tables.WORKSPACE.DATAPLANE_GROUP_ID,
              workspace.dataplaneGroupId,
            ).set(Tables.WORKSPACE.UPDATED_AT, timestamp)
            .set(
              Tables.WORKSPACE.WEBHOOK_OPERATION_CONFIGS,
              if (workspace.webhookOperationConfigs == null) {
                null
              } else {
                JSONB.valueOf(Jsons.serialize(workspace.webhookOperationConfigs))
              },
            ).set(Tables.WORKSPACE.ORGANIZATION_ID, workspace.organizationId)
            .where(Tables.WORKSPACE.ID.eq(workspace.workspaceId))
            .execute()
        } else {
          ctx
            .insertInto(Tables.WORKSPACE)
            .set(Tables.WORKSPACE.ID, workspace.workspaceId)
            .set(Tables.WORKSPACE.CUSTOMER_ID, workspace.customerId)
            .set(Tables.WORKSPACE.NAME, workspace.name)
            .set(Tables.WORKSPACE.SLUG, workspace.slug)
            .set(Tables.WORKSPACE.EMAIL, workspace.email)
            .set(
              Tables.WORKSPACE.INITIAL_SETUP_COMPLETE,
              workspace.initialSetupComplete,
            ).set(
              Tables.WORKSPACE.ANONYMOUS_DATA_COLLECTION,
              workspace.anonymousDataCollection,
            ).set(Tables.WORKSPACE.SEND_NEWSLETTER, workspace.news)
            .set(
              Tables.WORKSPACE.SEND_SECURITY_UPDATES,
              workspace.securityUpdates,
            ).set(
              Tables.WORKSPACE.DISPLAY_SETUP_WIZARD,
              workspace.displaySetupWizard,
            ).set(
              Tables.WORKSPACE.TOMBSTONE,
              workspace.tombstone != null && workspace.tombstone,
            ).set(
              Tables.WORKSPACE.NOTIFICATIONS,
              JSONB.valueOf(
                Jsons.serialize(workspace.notifications),
              ),
            ).set(
              Tables.WORKSPACE.NOTIFICATION_SETTINGS,
              JSONB.valueOf(Jsons.serialize(workspace.notificationSettings)),
            ).set(
              Tables.WORKSPACE.FIRST_SYNC_COMPLETE,
              workspace.firstCompletedSync,
            ).set(Tables.WORKSPACE.FEEDBACK_COMPLETE, workspace.feedbackDone)
            .set(Tables.WORKSPACE.CREATED_AT, timestamp)
            .set(Tables.WORKSPACE.UPDATED_AT, timestamp)
            .set(
              Tables.WORKSPACE.DATAPLANE_GROUP_ID,
              workspace.dataplaneGroupId,
            ).set(Tables.WORKSPACE.ORGANIZATION_ID, workspace.organizationId)
            .set(
              Tables.WORKSPACE.WEBHOOK_OPERATION_CONFIGS,
              if (workspace.webhookOperationConfigs == null) {
                null
              } else {
                JSONB.valueOf(Jsons.serialize(workspace.webhookOperationConfigs))
              },
            ).execute()
        }
        null
      }
    }

    /**
     * Set user feedback on workspace.
     *
     * @param workspaceId workspace id.
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class, ConfigNotFoundException::class)
    override fun setFeedback(workspaceId: UUID) {
      try {
        database.query { ctx: DSLContext ->
          ctx
            .update(Tables.WORKSPACE)
            .set(Tables.WORKSPACE.FEEDBACK_COMPLETE, true)
            .where(Tables.WORKSPACE.ID.eq(workspaceId))
            .execute()
        }
      } catch (e: DataAccessException) {
        throw ConfigNotFoundException("workspace", "Workspace not found")
      }
    }

    /**
     * Test if workspace id has access to a connector definition.
     *
     * @param actorDefinitionId actor definition id
     * @param workspaceId id of the workspace
     * @return true, if the workspace has access. otherwise, false.
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun workspaceCanUseDefinition(
      actorDefinitionId: UUID,
      workspaceId: UUID,
    ): Boolean = scopeCanUseDefinition(actorDefinitionId, workspaceId, ScopeType.workspace.toString())

    /**
     * Test if workspace has access to a custom connector definition.
     *
     * @param actorDefinitionId custom actor definition id
     * @param workspaceId workspace id
     * @return true, if the workspace has access. otherwise, false.
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun workspaceCanUseCustomDefinition(
      actorDefinitionId: UUID,
      workspaceId: UUID,
    ): Boolean {
      val records =
        actorDefinitionsJoinedWithGrants(
          workspaceId,
          ScopeType.workspace,
          JoinType.JOIN,
          Tables.ACTOR_DEFINITION.ID.eq(actorDefinitionId),
          Tables.ACTOR_DEFINITION.CUSTOM.eq(true),
        )
      return records.isNotEmpty
    }

    /**
     * List active workspace IDs with most recently running jobs within a given time window (in hours).
     *
     * @param timeWindowInHours - integer, e.g. 24, 48, etc
     * @return list of workspace IDs
     * @throws IOException - failed to query data
     */
    @Throws(IOException::class)
    override fun listActiveWorkspacesByMostRecentlyRunningJobs(timeWindowInHours: Int): List<UUID> {
      val records =
        database.query { ctx: DSLContext ->
          ctx
            .selectDistinct(Tables.ACTOR.WORKSPACE_ID)
            .from(Tables.ACTOR)
            .join(Tables.WORKSPACE)
            .on(Tables.ACTOR.WORKSPACE_ID.eq(Tables.WORKSPACE.ID))
            .join(Tables.CONNECTION)
            .on(Tables.CONNECTION.SOURCE_ID.eq(Tables.ACTOR.ID))
            .join(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
            .on(
              Tables.CONNECTION.ID
                .cast(SQLDataType.VARCHAR(255))
                .eq(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE),
            ).where(
              io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT.greaterOrEqual(
                OffsetDateTime.now().minusHours(timeWindowInHours.toLong()),
              ),
            ).and(Tables.WORKSPACE.TOMBSTONE.isFalse())
            .fetch()
        }
      return records.stream().map { record: Record1<UUID> -> record.get(Tables.ACTOR.WORKSPACE_ID) }.collect(Collectors.toList())
    }

    /**
     * Count connections in workspace.
     *
     * @param workspaceId workspace id
     * @return number of connections in workspace
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun countConnectionsForWorkspace(workspaceId: UUID): Int =
      database
        .query { ctx: DSLContext ->
          ctx
            .selectCount()
            .from(Tables.CONNECTION)
            .join(Tables.ACTOR)
            .on(Tables.CONNECTION.SOURCE_ID.eq(Tables.ACTOR.ID))
            .where(Tables.ACTOR.WORKSPACE_ID.eq(workspaceId))
            .and(Tables.CONNECTION.STATUS.notEqual(StatusType.deprecated))
            .andNot(Tables.ACTOR.TOMBSTONE)
        }.fetchOne()!!
        .into(Int::class.javaPrimitiveType)

    /**
     * Count sources in workspace.
     *
     * @param workspaceId workspace id
     * @return number of sources in workspace
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun countSourcesForWorkspace(workspaceId: UUID): Int =
      database
        .query { ctx: DSLContext ->
          ctx
            .selectCount()
            .from(Tables.ACTOR)
            .where(Tables.ACTOR.WORKSPACE_ID.equal(workspaceId))
            .and(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.source))
            .andNot(Tables.ACTOR.TOMBSTONE)
        }.fetchOne()!!
        .into(Int::class.javaPrimitiveType)

    /**
     * Count destinations in workspace.
     *
     * @param workspaceId workspace id
     * @return number of destinations in workspace
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun countDestinationsForWorkspace(workspaceId: UUID): Int =
      database
        .query { ctx: DSLContext ->
          ctx
            .selectCount()
            .from(Tables.ACTOR)
            .where(Tables.ACTOR.WORKSPACE_ID.equal(workspaceId))
            .and(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.destination))
            .andNot(Tables.ACTOR.TOMBSTONE)
        }.fetchOne()!!
        .into(Int::class.javaPrimitiveType)

    /**
     * Specialized query for efficiently determining eligibility for the Free Connector Program. If a
     * workspace has at least one Alpha or Beta connector, users of that workspace will be prompted to
     * sign up for the program. This check is performed on nearly every page load so the query needs to
     * be as efficient as possible.
     *
     *
     * This should only be used for efficiently determining eligibility for the Free Connector Program.
     * Anything that involves billing should instead use the ActorDefinitionVersionHelper to determine
     * the ReleaseStages.
     *
     * @param workspaceId ID of the workspace to check connectors for
     * @return boolean indicating if an alpha or beta connector exists within the workspace
     */
    @Throws(IOException::class)
    override fun getWorkspaceHasAlphaOrBetaConnector(workspaceId: UUID): Boolean {
      val releaseStageAlphaOrBeta =
        Tables.ACTOR_DEFINITION_VERSION.RELEASE_STAGE
          .eq(ReleaseStage.alpha)
          .or(Tables.ACTOR_DEFINITION_VERSION.RELEASE_STAGE.eq(ReleaseStage.beta))

      val countResult =
        database
          .query { ctx: DSLContext ->
            ctx
              .selectCount()
              .from(Tables.ACTOR)
              .join(Tables.ACTOR_DEFINITION)
              .on(Tables.ACTOR_DEFINITION.ID.eq(Tables.ACTOR.ACTOR_DEFINITION_ID))
              .join(Tables.ACTOR_DEFINITION_VERSION)
              .on(Tables.ACTOR_DEFINITION_VERSION.ID.eq(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID))
              .where(Tables.ACTOR.WORKSPACE_ID.eq(workspaceId))
              .and(Tables.ACTOR.TOMBSTONE.notEqual(true))
              .and(releaseStageAlphaOrBeta)
          }.fetchOneInto(Int::class.java)

      return countResult!! > 0
    }

    /**
     * List connection IDs for active syncs based on the given query.
     *
     * @param standardSyncQuery query
     * @return list of connection IDs
     * @throws IOException if there is an issue while interacting with db.
     */
    @Throws(IOException::class)
    override fun listWorkspaceActiveSyncIds(standardSyncQuery: StandardSyncQuery): List<UUID> =
      database
        .query { ctx: DSLContext ->
          ctx
            .select(Tables.CONNECTION.ID)
            .from(Tables.CONNECTION)
            .join(Tables.ACTOR)
            .on(Tables.CONNECTION.SOURCE_ID.eq(Tables.ACTOR.ID))
            .where(
              Tables.ACTOR.WORKSPACE_ID
                .eq(standardSyncQuery.workspaceId)
                .and(
                  if (standardSyncQuery.destinationId == null || standardSyncQuery.destinationId.isEmpty()) {
                    DSL.noCondition()
                  } else {
                    Tables.CONNECTION.DESTINATION_ID.`in`(standardSyncQuery.destinationId)
                  },
                ).and(
                  if (standardSyncQuery.sourceId == null || standardSyncQuery.sourceId.isEmpty()) {
                    DSL.noCondition()
                  } else {
                    Tables.CONNECTION.SOURCE_ID.`in`(standardSyncQuery.sourceId)
                  },
                ) // includeDeleted is not relevant here because it refers to connection status deprecated,
                // and we are only retrieving active syncs anyway
                .and(Tables.CONNECTION.STATUS.eq(StatusType.active)),
            ).groupBy(Tables.CONNECTION.ID)
        }.fetchInto(UUID::class.java)

    /**
     * List workspaces with given ids.
     *
     * @param includeTombstone include tombstoned workspaces
     * @return workspaces
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun listStandardWorkspacesWithIds(
      workspaceIds: List<UUID>,
      includeTombstone: Boolean,
    ): List<StandardWorkspace> = listWorkspaceQuery(Optional.of(workspaceIds), includeTombstone).toList()

    /**
     * Returns source with a given id. Does not contain secrets. To hydrate with secrets see { @link
     * SourceService#getSourceConnectionWithSecrets(final UUID sourceId) }.
     *
     * @param sourceId - id of source to fetch.
     * @return sources
     * @throws JsonValidationException - throws if returned sources are invalid
     * @throws IOException - you never know when you IO
     * @throws ConfigNotFoundException - throws if no source with that id can be found.
     */
    @VisibleForTesting
    @Throws(
      JsonValidationException::class,
      ConfigNotFoundException::class,
      IOException::class,
    )
    fun getSourceConnection(sourceId: UUID): SourceConnection =
      listSourceQuery(Optional.of(sourceId))
        .findFirst()
        .orElseThrow {
          ConfigNotFoundException(
            ConfigNotFoundType.SOURCE_CONNECTION,
            sourceId,
          )
        }

    /**
     * Test if workspace or organization id has access to a connector definition.
     *
     * @param actorDefinitionId actor definition id
     * @param scopeId id of the workspace or organization
     * @param scopeType enum of workspace or organization
     * @return true, if the workspace or organization has access. otherwise, false.
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    private fun scopeCanUseDefinition(
      actorDefinitionId: UUID,
      scopeId: UUID,
      scopeType: String,
    ): Boolean {
      val records =
        actorDefinitionsJoinedWithGrants(
          scopeId,
          ScopeType.valueOf(scopeType),
          JoinType.LEFT_OUTER_JOIN,
          Tables.ACTOR_DEFINITION.ID.eq(actorDefinitionId),
          Tables.ACTOR_DEFINITION.PUBLIC
            .eq(true)
            .or(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID.eq(actorDefinitionId)),
        )
      return records.isNotEmpty
    }

    @Throws(IOException::class)
    private fun listSourceQuery(configId: Optional<UUID>): Stream<SourceConnection> {
      val result =
        database.query { ctx: DSLContext ->
          val query = ctx.select(DSL.asterisk()).from(Tables.ACTOR)
          if (configId.isPresent) {
            return@query query
              .where(
                Tables.ACTOR.ACTOR_TYPE.eq(ActorType.source),
                Tables.ACTOR.ID.eq(configId.get()),
              ).fetch()
          }
          query
            .where(Tables.ACTOR.ACTOR_TYPE.eq(ActorType.source))
            .fetch()
        }

      return result.map { record: Record -> DbConverter.buildSourceConnection(record) }.stream()
    }

    @Throws(IOException::class)
    private fun actorDefinitionsJoinedWithGrants(
      scopeId: UUID,
      scopeType: ScopeType,
      joinType: JoinType,
      vararg conditions: Condition,
    ): Result<Record> {
      var scopeConditional =
        Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE.eq(ScopeType.valueOf(scopeType.toString())).and(
          Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(scopeId),
        )

      // if scope type is workspace, get organization id as well and add that into OR conditional
      if (scopeType == ScopeType.workspace) {
        val organizationId = getOrganizationIdFromWorkspaceId(scopeId)
        if (organizationId.isPresent) {
          scopeConditional =
            scopeConditional.or(
              Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE.eq(ScopeType.organization).and(
                Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(organizationId.get()),
              ),
            )
        }
      }

      val finalScopeConditional = scopeConditional
      return database.query { ctx: DSLContext ->
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
      }
    }

    @Throws(IOException::class)
    override fun getOrganizationIdFromWorkspaceId(scopeId: UUID?): Optional<UUID> {
      if (scopeId == null) {
        return Optional.empty()
      }
      val optionalRecord =
        database.query { ctx: DSLContext ->
          ctx
            .select(Tables.WORKSPACE.ORGANIZATION_ID)
            .from(Tables.WORKSPACE)
            .where(Tables.WORKSPACE.ID.eq(scopeId))
            .fetchOptional()
        }
      return optionalRecord.map { obj: Record1<UUID> -> obj.value1() }
    }

    /**
     * Get workspace with secrets.
     *
     * @param workspaceId workspace id
     * @param includeTombstone include workspace even if it is tombstoned
     * @return workspace with secrets
     * @throws JsonValidationException if the workspace is or contains invalid json
     * @throws ConfigNotFoundException if the config does not exist
     * @throws IOException if there is an issue while interacting with the secrets store or db.
     */
    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    override fun getWorkspaceWithSecrets(
      workspaceId: UUID,
      includeTombstone: Boolean,
    ): StandardWorkspace {
      val workspace = getStandardWorkspaceNoSecrets(workspaceId, includeTombstone)
      val webhookConfigs: JsonNode?
      val organizationId = workspace.organizationId
      if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId))) {
        val secretPersistenceConfig =
          secretPersistenceConfigService.get(io.airbyte.config.ScopeType.ORGANIZATION, organizationId)
        webhookConfigs =
          secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(
            workspace.webhookOperationConfigs,
            RuntimeSecretPersistence(secretPersistenceConfig, metricClient),
          )
      } else {
        webhookConfigs = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(workspace.webhookOperationConfigs)
      }
      workspace.withWebhookOperationConfigs(webhookConfigs)
      return workspace
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    override fun writeWorkspaceWithSecrets(workspace: StandardWorkspace) {
      // Get the schema for the webhook config, so we can split out any secret fields.
      val webhookConfigSchema = Yamls.deserialize(Resources.read("types/WebhookOperationConfigs.yaml"))
      // Check if there's an existing config, so we can re-use the secret coordinates.
      val previousWorkspace = getWorkspaceIfExists(workspace.workspaceId)
      var previousWebhookConfigs: Optional<JsonNode> = Optional.empty()

      if (previousWorkspace.isPresent && previousWorkspace.get().webhookOperationConfigs != null) {
        previousWebhookConfigs = Optional.of(previousWorkspace.get().webhookOperationConfigs)
      }

      val partialWorkspace = Jsons.clone(workspace)

      if (workspace.webhookOperationConfigs != null) {
        val organizationId = workspace.organizationId
        var secretPersistence: RuntimeSecretPersistence? = null

        if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId))) {
          val secretPersistenceConfig =
            secretPersistenceConfigService.get(io.airbyte.config.ScopeType.ORGANIZATION, organizationId)
          secretPersistence = RuntimeSecretPersistence(secretPersistenceConfig, metricClient)
        }
        val partialConfig =
          if (previousWebhookConfigs.isPresent) {
            secretsRepositoryWriter.updateFromConfigLegacy(
              workspace.workspaceId,
              previousWebhookConfigs.get(),
              workspace.webhookOperationConfigs,
              webhookConfigSchema,
              secretPersistence,
            )
          } else {
            secretsRepositoryWriter.createFromConfigLegacy(
              workspace.workspaceId,
              workspace.webhookOperationConfigs,
              webhookConfigSchema,
              secretPersistence,
            )
          }
        partialWorkspace.withWebhookOperationConfigs(partialConfig)
      }

      writeStandardWorkspaceNoSecrets(partialWorkspace)
    }

    private fun getWorkspaceIfExists(workspaceId: UUID): Optional<StandardWorkspace> {
      try {
        return Optional.of(getStandardWorkspaceNoSecrets(workspaceId, false))
      } catch (e: ConfigNotFoundException) {
        log.warn("Unable to find workspace with ID {}", workspaceId)
        return Optional.empty()
      } catch (e: JsonValidationException) {
        log.warn("Unable to find workspace with ID {}", workspaceId)
        return Optional.empty()
      } catch (e: IOException) {
        log.warn("Unable to find workspace with ID {}", workspaceId)
        return Optional.empty()
      }
    }
  }
