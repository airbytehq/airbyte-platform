/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.constants.AirbyteCatalogConstants
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ScopeType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.shared.ActorWorkspaceOrganizationIds
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportState
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorDefinitionBreakingChangeRecord
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorDefinitionVersionRecord
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.JoinType
import org.jooq.Record
import org.jooq.Record1
import org.jooq.Record3
import org.jooq.Record4
import org.jooq.Result
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.stream.Collectors
import java.util.stream.Stream

@Singleton
class ActorDefinitionServiceJooqImpl
  @VisibleForTesting
  constructor(
    @Named("configDatabase") database: Database?,
  ) : ActorDefinitionService {
    private val database = ExceptionWrappingDatabase(database)

    /**
     * Get actor definition IDs that are in use.
     *
     * @return list of IDs
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun getActorDefinitionIdsInUse(): Set<UUID> =
      database.query { ctx: DSLContext ->
        getActorDefinitionsInUse(ctx)
          .map { r: Record4<UUID, String, ActorType, String> ->
            r.get(
              Tables.ACTOR_DEFINITION.ID,
            )
          }.collect(Collectors.toSet())
      }

    /**
     * Get actor definition ids to pair of actor type and protocol version.
     *
     * @return map of definition id to pair of actor type and protocol version.
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun getActorDefinitionToProtocolVersionMap(): Map<UUID, Map.Entry<io.airbyte.config.ActorType, Version>> =
      database.query { ctx: DSLContext ->
        getActorDefinitionsInUse(ctx)
          .collect(
            Collectors.toMap(
              { r: Record4<UUID, String, ActorType, String> ->
                r.get(
                  Tables.ACTOR_DEFINITION.ID,
                )
              },
              { r: Record4<UUID, String, ActorType, String> ->
                java.util.Map.entry(
                  if (r.get(Tables.ACTOR_DEFINITION.ACTOR_TYPE) == ActorType.source) {
                    io.airbyte.config.ActorType.SOURCE
                  } else {
                    io.airbyte.config.ActorType.DESTINATION
                  },
                  AirbyteProtocolVersion.getWithDefault(r.get(Tables.ACTOR_DEFINITION_VERSION.PROTOCOL_VERSION)),
                )
              }, // We may have duplicated entries from the data. We can pick any values in the merge function
              { lhs: Map.Entry<io.airbyte.config.ActorType, Version>, rhs: Map.Entry<io.airbyte.config.ActorType, Version>? -> lhs },
            ),
          )
      }

    /**
     * Get a map of all actor definition ids and their default versions.
     *
     * @return map of definition id to default version.
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun getActorDefinitionIdsToDefaultVersionsMap(): Map<UUID, ActorDefinitionVersion> =
      database.query { ctx: DSLContext ->
        ctx
          .select(
            Tables.ACTOR_DEFINITION.ID,
            Tables.ACTOR_DEFINITION_VERSION.asterisk(),
          ).from(Tables.ACTOR_DEFINITION)
          .join(Tables.ACTOR_DEFINITION_VERSION)
          .on(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID.eq(Tables.ACTOR_DEFINITION_VERSION.ID))
          .fetch()
          .stream()
          .collect(
            Collectors.toMap(
              { record: Record -> record.get(Tables.ACTOR_DEFINITION.ID) },
              { record: Record ->
                DbConverter.buildActorDefinitionVersion(
                  record,
                )
              },
            ),
          )
      }

    /**
     * Update the docker image tag for multiple source-declarative-manifest actor definition versions at
     * once.
     *
     * @param currentImageTag the current docker image tag for these actor definition versions
     * @param targetImageTag the new docker image tag for these actor definition versions
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun updateDeclarativeActorDefinitionVersions(
      currentImageTag: String,
      targetImageTag: String,
    ): Int =
      database.transaction { ctx: DSLContext ->
        updateDeclarativeSourceVersionsImageTags(
          currentImageTag,
          targetImageTag,
          ctx,
        )
      }

    /**
     * Write actor definition workspace grant.
     *
     * @param actorDefinitionId actor definition id
     * @param scopeId workspace or organization id
     * @param scopeType ScopeType of either workspace or organization
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun writeActorDefinitionWorkspaceGrant(
      actorDefinitionId: UUID,
      scopeId: UUID,
      scopeType: ScopeType,
    ) {
      database.query { ctx: DSLContext ->
        writeActorDefinitionWorkspaceGrant(
          actorDefinitionId,
          scopeId,
          io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
            .valueOf(scopeType.value()),
          ctx,
        )
      }
    }

    /**
     * Test if grant exists for actor definition and scope.
     *
     * @param actorDefinitionId actor definition id
     * @param scopeId workspace or organization id
     * @param scopeType enum of workspace or organization
     * @return true, if the scope has access. otherwise, false.
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun actorDefinitionWorkspaceGrantExists(
      actorDefinitionId: UUID,
      scopeId: UUID,
      scopeType: ScopeType,
    ): Boolean {
      val count =
        database.query { ctx: DSLContext ->
          ctx.fetchCount(
            DSL
              .selectFrom(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT)
              .where(
                Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID.eq(
                  actorDefinitionId,
                ),
              ).and(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(scopeId))
              .and(
                Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE.eq(
                  io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
                    .valueOf(scopeType.value()),
                ),
              ),
          )
        }
      return count == 1
    }

    /**
     * Delete workspace access to actor definition.
     *
     * @param actorDefinitionId actor definition id to remove
     * @param scopeId workspace or organization id
     * @param scopeType enum of workspace or organization
     * @throws IOException - you never know when you IO
     */
    @Throws(IOException::class)
    override fun deleteActorDefinitionWorkspaceGrant(
      actorDefinitionId: UUID,
      scopeId: UUID,
      scopeType: ScopeType,
    ) {
      database.query { ctx: DSLContext ->
        ctx
          .deleteFrom(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT)
          .where(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
          .and(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(scopeId))
          .and(
            Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE.eq(
              io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
                .valueOf(scopeType.value()),
            ),
          ).execute()
      }
    }

    /**
     * Insert an actor definition version.
     *
     * @param actorDefinitionVersion - actor definition version to insert
     * @throws IOException - you never know when you io
     * @returns the POJO associated with the actor definition version inserted. Contains the versionId
     * field from the DB.
     */
    @Throws(IOException::class)
    override fun writeActorDefinitionVersion(actorDefinitionVersion: ActorDefinitionVersion): ActorDefinitionVersion =
      database.transaction { ctx: DSLContext ->
        writeActorDefinitionVersion(
          actorDefinitionVersion,
          ctx,
        )
      }

    /**
     * Get the actor definition version associated with an actor definition and a docker image tag.
     *
     * @param actorDefinitionId - actor definition id
     * @param dockerImageTag - docker image tag
     * @return actor definition version if there is an entry in the DB already for this version,
     * otherwise an empty optional
     * @throws IOException - you never know when you io
     */
    @Throws(IOException::class)
    override fun getActorDefinitionVersion(
      actorDefinitionId: UUID,
      dockerImageTag: String,
    ): Optional<ActorDefinitionVersion> =
      database.query { ctx: DSLContext ->
        getActorDefinitionVersion(
          actorDefinitionId,
          dockerImageTag,
          ctx,
        )
      }

    /**
     * Get an actor definition version by ID.
     *
     * @param actorDefinitionVersionId - actor definition version id
     * @return actor definition version
     * @throws ConfigNotFoundException if an actor definition version with the provided ID does not
     * exist
     * @throws IOException - you never know when you io
     */
    @Throws(IOException::class, ConfigNotFoundException::class)
    override fun getActorDefinitionVersion(actorDefinitionVersionId: UUID): ActorDefinitionVersion =
      getActorDefinitionVersions(java.util.List.of(actorDefinitionVersionId))
        .stream()
        .findFirst()
        .orElseThrow {
          ConfigNotFoundException(
            ConfigNotFoundType.ACTOR_DEFINITION_VERSION,
            actorDefinitionVersionId.toString(),
          )
        }

    /**
     * List all actor definition versions for a given actor definition.
     *
     * @param actorDefinitionId - actor definition id
     * @return list of actor definition versions
     * @throws IOException - you never know when you io
     */
    @Throws(IOException::class)
    override fun listActorDefinitionVersionsForDefinition(actorDefinitionId: UUID): List<ActorDefinitionVersion> =
      database.query { ctx: DSLContext ->
        ctx
          .selectFrom(Tables.ACTOR_DEFINITION_VERSION)
          .where(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
          .fetch()
          .stream()
          .map { record: ActorDefinitionVersionRecord ->
            DbConverter.buildActorDefinitionVersion(
              record,
            )
          }.collect(Collectors.toList())
      }

    /**
     * Get actor definition versions by ID.
     *
     * @param actorDefinitionVersionIds - actor definition version ids
     * @return list of actor definition version
     * @throws IOException - you never know when you io
     */
    @Throws(IOException::class)
    override fun getActorDefinitionVersions(actorDefinitionVersionIds: List<UUID?>): List<ActorDefinitionVersion> =
      database
        .query { ctx: DSLContext ->
          ctx.selectFrom(
            Tables.ACTOR_DEFINITION_VERSION,
          )
        }.where(Tables.ACTOR_DEFINITION_VERSION.ID.`in`(actorDefinitionVersionIds))
        .fetch()
        .stream()
        .map { record: ActorDefinitionVersionRecord -> DbConverter.buildActorDefinitionVersion(record) }
        .collect(Collectors.toList())

    @Throws(IOException::class)
    override fun getActorIdsForDefinition(actorDefinitionId: UUID): List<ActorWorkspaceOrganizationIds> =
      database.query { ctx: DSLContext ->
        ctx
          .select(
            Tables.ACTOR.ID,
            Tables.ACTOR.WORKSPACE_ID,
            Tables.WORKSPACE.ORGANIZATION_ID,
          ).from(Tables.ACTOR)
          .join(Tables.WORKSPACE)
          .on(Tables.ACTOR.WORKSPACE_ID.eq(Tables.WORKSPACE.ID))
          .where(Tables.ACTOR.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
          .fetch()
          .stream()
          .map { record: Record3<UUID, UUID, UUID> ->
            ActorWorkspaceOrganizationIds(
              record.get(Tables.ACTOR.ID),
              record.get(Tables.ACTOR.WORKSPACE_ID),
              record.get(Tables.WORKSPACE.ORGANIZATION_ID),
            )
          }.toList()
      }

    @Throws(IOException::class)
    override fun getIdsForActors(actorIds: List<UUID>): List<ActorWorkspaceOrganizationIds> =
      database.query { ctx: DSLContext ->
        ctx
          .select(
            Tables.ACTOR.ID,
            Tables.ACTOR.WORKSPACE_ID,
            Tables.WORKSPACE.ORGANIZATION_ID,
          ).from(Tables.ACTOR)
          .join(Tables.WORKSPACE)
          .on(Tables.ACTOR.WORKSPACE_ID.eq(Tables.WORKSPACE.ID))
          .where(Tables.ACTOR.ID.`in`(actorIds))
          .fetch()
          .stream()
          .map { record: Record3<UUID, UUID, UUID> ->
            ActorWorkspaceOrganizationIds(
              record.get(Tables.ACTOR.ID),
              record.get(Tables.ACTOR.WORKSPACE_ID),
              record.get(Tables.WORKSPACE.ORGANIZATION_ID),
            )
          }.toList()
      }

    @Throws(IOException::class)
    override fun updateActorDefinitionDefaultVersionId(
      actorDefinitionId: UUID,
      versionId: UUID,
    ) {
      database.query { ctx: DSLContext ->
        ctx
          .update(Tables.ACTOR_DEFINITION)
          .set(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID, versionId)
          .set(
            Tables.ACTOR_DEFINITION.UPDATED_AT,
            OffsetDateTime.now(),
          ).where(Tables.ACTOR_DEFINITION.ID.eq(actorDefinitionId))
          .execute()
      }
    }

    /**
     * Get the list of breaking changes available affecting an actor definition.
     *
     * @param actorDefinitionId - actor definition id
     * @return list of breaking changes
     * @throws IOException - you never know when you io
     */
    @Throws(IOException::class)
    override fun listBreakingChangesForActorDefinition(actorDefinitionId: UUID): List<ActorDefinitionBreakingChange> =
      database.query { ctx: DSLContext ->
        listBreakingChangesForActorDefinition(
          actorDefinitionId,
          ctx,
        )
      }

    /**
     * Set the support state for a list of actor definition versions.
     *
     * @param actorDefinitionVersionIds - actor definition version ids to update
     * @param supportState - support state to update to
     * @throws IOException - you never know when you io
     */
    @Throws(IOException::class)
    override fun setActorDefinitionVersionSupportStates(
      actorDefinitionVersionIds: List<UUID>,
      supportState: ActorDefinitionVersion.SupportState,
    ) {
      database.query { ctx: DSLContext ->
        ctx
          .update(Tables.ACTOR_DEFINITION_VERSION)
          .set(
            Tables.ACTOR_DEFINITION_VERSION.SUPPORT_STATE,
            supportState.value().toEnum<SupportState>()!!,
          ).set(
            Tables.ACTOR_DEFINITION_VERSION.UPDATED_AT,
            OffsetDateTime.now(),
          ).where(Tables.ACTOR_DEFINITION_VERSION.ID.`in`(actorDefinitionVersionIds))
          .execute()
      }
    }

    /**
     * Get the list of breaking changes available affecting an actor definition version.
     *
     *
     * "Affecting" breaking changes are those between the provided version (non-inclusive) and the actor
     * definition default version (inclusive).
     *
     * @param actorDefinitionVersion - actor definition version
     * @return list of breaking changes
     * @throws IOException - you never know when you io
     */
    @Throws(IOException::class)
    override fun listBreakingChangesForActorDefinitionVersion(actorDefinitionVersion: ActorDefinitionVersion): List<ActorDefinitionBreakingChange> {
      val breakingChanges = listBreakingChangesForActorDefinition(actorDefinitionVersion.actorDefinitionId)
      if (breakingChanges.isEmpty()) {
        return listOf()
      }

      val currentVersion = Version(actorDefinitionVersion.dockerImageTag)
      val latestVersion =
        Version(getDefaultVersionForActorDefinitionId(actorDefinitionVersion.actorDefinitionId).dockerImageTag)

      return breakingChanges
        .stream()
        .filter { breakingChange: ActorDefinitionBreakingChange ->
          breakingChange.version.greaterThan(currentVersion) &&
            latestVersion.greaterThanOrEqualTo(breakingChange.version)
        }.sorted { v1: ActorDefinitionBreakingChange, v2: ActorDefinitionBreakingChange -> v1.version.versionCompareTo(v2.version) }
        .toList()
    }

    /**
     * List all breaking changes.
     *
     * @return list of breaking changes
     * @throws IOException - you never know when you io
     */
    @Throws(IOException::class)
    override fun listBreakingChanges(): List<ActorDefinitionBreakingChange> =
      database.query { ctx: DSLContext ->
        ctx
          .selectFrom(Tables.ACTOR_DEFINITION_BREAKING_CHANGE)
          .fetch()
          .stream()
          .map { record: ActorDefinitionBreakingChangeRecord ->
            DbConverter.buildActorDefinitionBreakingChange(
              record,
            )
          }.collect(Collectors.toList())
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
    override fun scopeCanUseDefinition(
      actorDefinitionId: UUID,
      scopeId: UUID,
      scopeType: String,
    ): Boolean {
      val records =
        actorDefinitionsJoinedWithGrants(
          scopeId,
          io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
            .valueOf(scopeType),
          JoinType.LEFT_OUTER_JOIN,
          Tables.ACTOR_DEFINITION.ID.eq(actorDefinitionId),
          Tables.ACTOR_DEFINITION.PUBLIC
            .eq(true)
            .or(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID.eq(actorDefinitionId)),
        )
      return records.isNotEmpty
    }

    @Throws(IOException::class)
    private fun getOrganizationIdFromWorkspaceId(scopeId: UUID): Optional<UUID> {
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

    @Throws(IOException::class)
    private fun actorDefinitionsJoinedWithGrants(
      scopeId: UUID,
      scopeType: io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType,
      joinType: JoinType,
      vararg conditions: Condition,
    ): Result<Record> {
      var scopeConditional =
        Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE
          .eq(
            io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
              .valueOf(scopeType.toString()),
          ).and(
            Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(scopeId),
          )

      // if scope type is workspace, get organization id as well and add that into OR conditional
      if (scopeType == io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.workspace) {
        val organizationId = getOrganizationIdFromWorkspaceId(scopeId)
        if (organizationId.isPresent) {
          scopeConditional =
            scopeConditional.or(
              Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE
                .eq(
                  io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.organization,
                ).and(
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

    private fun updateDeclarativeSourceVersionsImageTags(
      currentImageTag: String,
      targetImageTag: String,
      ctx: DSLContext,
    ): Int {
      val timestamp = OffsetDateTime.now()

      // We are updating the actor definition version itself instead of changing the actor definition's
      // default version because connector builder projects have a different concept of versioning
      return ctx
        .update(Tables.ACTOR_DEFINITION_VERSION)
        .set(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, targetImageTag)
        .set(Tables.ACTOR_DEFINITION_VERSION.UPDATED_AT, timestamp)
        .where(
          Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY
            .equal(AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE)
            .and(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG.equal(currentImageTag)),
        ).execute()
    }

    private fun writeActorDefinitionWorkspaceGrant(
      actorDefinitionId: UUID,
      scopeId: UUID,
      scopeType: io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType,
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
      if (scopeType == io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType.workspace) {
        insertStep = insertStep.set(Tables.ACTOR_DEFINITION_WORKSPACE_GRANT.WORKSPACE_ID, scopeId)
      }
      return insertStep.execute()
    }

    private fun listBreakingChangesForActorDefinition(
      actorDefinitionId: UUID,
      ctx: DSLContext,
    ): List<ActorDefinitionBreakingChange> =
      ctx
        .selectFrom(Tables.ACTOR_DEFINITION_BREAKING_CHANGE)
        .where(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
        .fetch()
        .stream()
        .map { record: ActorDefinitionBreakingChangeRecord -> DbConverter.buildActorDefinitionBreakingChange(record) }
        .collect(Collectors.toList())

    @Throws(IOException::class)
    private fun getDefaultVersionForActorDefinitionId(actorDefinitionId: UUID): ActorDefinitionVersion =
      database.query { ctx: DSLContext ->
        getDefaultVersionForActorDefinitionId(
          actorDefinitionId,
          ctx,
        )
      }

    private fun getDefaultVersionForActorDefinitionId(
      actorDefinitionId: UUID,
      ctx: DSLContext,
    ): ActorDefinitionVersion = ConnectorMetadataJooqHelper.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId, ctx).orElseThrow()

    /**
     * Get an optional ADV for an actor definition's default version. The optional will be empty if the
     * defaultVersionId of the actor definition is set to null in the DB. The only time this should be
     * the case is if we are in the process of inserting and have already written the source definition,
     * but not yet set its default version.
     */
    @Throws(IOException::class)
    override fun getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId: UUID): Optional<ActorDefinitionVersion> =
      database.query { ctx: DSLContext ->
        ConnectorMetadataJooqHelper.getDefaultVersionForActorDefinitionIdOptional(
          actorDefinitionId,
          ctx,
        )
      }

    /**
     * Insert an actor definition version.
     *
     * @param actorDefinitionVersion - actor definition version to insert
     * @param ctx database context
     * @throws IOException - you never know when you io
     * @returns the POJO associated with the actor definition version inserted. Contains the versionId
     * field from the DB.
     */
    private fun writeActorDefinitionVersion(
      actorDefinitionVersion: ActorDefinitionVersion,
      ctx: DSLContext,
    ): ActorDefinitionVersion = ConnectorMetadataJooqHelper.writeActorDefinitionVersion(actorDefinitionVersion, ctx)

    /**
     * Get the actor definition version associated with an actor definition and a docker image tag.
     *
     * @param actorDefinitionId - actor definition id
     * @param dockerImageTag - docker image tag
     * @param ctx database context
     * @return actor definition version if there is an entry in the DB already for this version,
     * otherwise an empty optional
     * @throws IOException - you never know when you io
     */
    private fun getActorDefinitionVersion(
      actorDefinitionId: UUID,
      dockerImageTag: String,
      ctx: DSLContext,
    ): Optional<ActorDefinitionVersion> =
      ctx
        .selectFrom(Tables.ACTOR_DEFINITION_VERSION)
        .where(
          Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID
            .eq(actorDefinitionId)
            .and(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG.eq(dockerImageTag)),
        ).fetch()
        .stream()
        .findFirst()
        .map { record: ActorDefinitionVersionRecord -> DbConverter.buildActorDefinitionVersion(record) }

    companion object {
      private fun getActorDefinitionsInUse(ctx: DSLContext): Stream<Record4<UUID, String, ActorType, String>> =
        ctx
          .selectDistinct(
            Tables.ACTOR_DEFINITION.ID,
            Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY,
            Tables.ACTOR_DEFINITION.ACTOR_TYPE,
            Tables.ACTOR_DEFINITION_VERSION.PROTOCOL_VERSION,
          ).from(Tables.ACTOR_DEFINITION)
          .join(Tables.ACTOR)
          .on(Tables.ACTOR.ACTOR_DEFINITION_ID.equal(Tables.ACTOR_DEFINITION.ID))
          .join(Tables.ACTOR_DEFINITION_VERSION)
          .on(Tables.ACTOR_DEFINITION_VERSION.ID.equal(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID))
          .fetch()
          .stream()
    }
  }
