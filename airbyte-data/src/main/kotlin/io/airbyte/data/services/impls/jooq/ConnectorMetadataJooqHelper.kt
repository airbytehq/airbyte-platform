/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportState
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorDefinitionVersionRecord
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Query
import org.jooq.Record
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import java.util.UUID
import java.util.stream.Collectors

/**
 * Helper class used to read/write connector metadata from associated tables (breaking changes,
 * actor definition versions).
 */
object ConnectorMetadataJooqHelper {
  /**
   * Write an actor definition version.
   *
   * @param actorDefinitionVersion - actor definition version to write
   * @param ctx database context
   * @return the POJO associated with the actor definition version written. Contains the versionId
   * field from the DB.
   */
  fun writeActorDefinitionVersion(
    actorDefinitionVersion: ActorDefinitionVersion,
    ctx: DSLContext,
  ): ActorDefinitionVersion {
    val timestamp = OffsetDateTime.now()

    val providedVersionId = Optional.ofNullable(actorDefinitionVersion.versionId)

    val actorDefinitionId = actorDefinitionVersion.actorDefinitionId
    val dockerImageTag = actorDefinitionVersion.dockerImageTag
    val existingADV = getActorDefinitionVersion(actorDefinitionId, dockerImageTag, ctx)

    if (providedVersionId.isPresent && existingADV.isPresent && (existingADV.get().versionId != providedVersionId.get())) {
      throw RuntimeException(
        String.format(
          "Provided version id %s does not match existing version id %s for actor definition %s and docker image tag %s",
          providedVersionId.get(),
          existingADV.get().versionId,
          actorDefinitionId,
          dockerImageTag,
        ),
      )
    }

    if (existingADV.isPresent) {
      val versionId = existingADV.get().versionId
      actorDefinitionVersion.versionId = versionId

      ctx
        .update(Tables.ACTOR_DEFINITION_VERSION)
        .set(Tables.ACTOR_DEFINITION_VERSION.UPDATED_AT, timestamp)
        .set(Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, actorDefinitionVersion.dockerRepository)
        .set(Tables.ACTOR_DEFINITION_VERSION.SPEC, JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.spec)))
        .set(Tables.ACTOR_DEFINITION_VERSION.DOCUMENTATION_URL, actorDefinitionVersion.documentationUrl)
        .set(Tables.ACTOR_DEFINITION_VERSION.PROTOCOL_VERSION, actorDefinitionVersion.protocolVersion)
        .set(
          Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL,
          if (actorDefinitionVersion.supportLevel == null) {
            null
          } else {
            actorDefinitionVersion.supportLevel.value().toEnum<SupportLevel>()!!
          },
        ).set(
          Tables.ACTOR_DEFINITION_VERSION.RELEASE_STAGE,
          if (actorDefinitionVersion.releaseStage == null) {
            null
          } else {
            actorDefinitionVersion.releaseStage.value().toEnum<ReleaseStage>()!!
          },
        ).set(
          Tables.ACTOR_DEFINITION_VERSION.RELEASE_DATE,
          if (actorDefinitionVersion.releaseDate == null) {
            null
          } else {
            LocalDate.parse(actorDefinitionVersion.releaseDate)
          },
        ).set(
          Tables.ACTOR_DEFINITION_VERSION.ALLOWED_HOSTS,
          if (actorDefinitionVersion.allowedHosts == null) {
            null
          } else {
            JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.allowedHosts))
          },
        ).set(
          Tables.ACTOR_DEFINITION_VERSION.SUGGESTED_STREAMS,
          if (actorDefinitionVersion.suggestedStreams == null) {
            null
          } else {
            JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.suggestedStreams))
          },
        ).set(
          Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_REFRESHES,
          actorDefinitionVersion.supportsRefreshes != null && actorDefinitionVersion.supportsRefreshes,
        ).set(
          Tables.ACTOR_DEFINITION_VERSION.SUPPORT_STATE,
          actorDefinitionVersion.supportState.value().toEnum<SupportState>()!!,
        ).set(
          Tables.ACTOR_DEFINITION_VERSION.LAST_PUBLISHED,
          if (actorDefinitionVersion.lastPublished == null) {
            null
          } else {
            actorDefinitionVersion.lastPublished.toInstant().atOffset(ZoneOffset.UTC)
          },
        ).set(Tables.ACTOR_DEFINITION_VERSION.CDK_VERSION, actorDefinitionVersion.cdkVersion)
        .set(Tables.ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL, actorDefinitionVersion.internalSupportLevel)
        .set(Tables.ACTOR_DEFINITION_VERSION.LANGUAGE, actorDefinitionVersion.language)
        .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_FILE_TRANSFER, actorDefinitionVersion.supportsFileTransfer)
        .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_DATA_ACTIVATION, actorDefinitionVersion.supportsDataActivation)
        .set(
          Tables.ACTOR_DEFINITION_VERSION.CONNECTOR_IPC_OPTIONS,
          if (actorDefinitionVersion.connectorIPCOptions == null) {
            null
          } else {
            JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.connectorIPCOptions))
          },
        ).where(Tables.ACTOR_DEFINITION_VERSION.ID.eq(versionId))
        .execute()
    } else {
      // If the version id is provided, use it (useful for mocks). Otherwise, generate a new one.
      val versionId = providedVersionId.orElse(UUID.randomUUID())
      actorDefinitionVersion.versionId = versionId

      ctx
        .insertInto(Tables.ACTOR_DEFINITION_VERSION)
        .set(Tables.ACTOR_DEFINITION_VERSION.ID, versionId)
        .set(Tables.ACTOR_DEFINITION_VERSION.CREATED_AT, timestamp)
        .set(Tables.ACTOR_DEFINITION_VERSION.UPDATED_AT, timestamp)
        .set(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID, actorDefinitionVersion.actorDefinitionId)
        .set(Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, actorDefinitionVersion.dockerRepository)
        .set(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, actorDefinitionVersion.dockerImageTag)
        .set(Tables.ACTOR_DEFINITION_VERSION.SPEC, JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.spec)))
        .set(Tables.ACTOR_DEFINITION_VERSION.DOCUMENTATION_URL, actorDefinitionVersion.documentationUrl)
        .set(Tables.ACTOR_DEFINITION_VERSION.PROTOCOL_VERSION, actorDefinitionVersion.protocolVersion)
        .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL, actorDefinitionVersion.supportLevel?.value()?.toEnum<SupportLevel>())
        .set(Tables.ACTOR_DEFINITION_VERSION.RELEASE_STAGE, actorDefinitionVersion.releaseStage?.value()?.toEnum<ReleaseStage>())
        .set(
          Tables.ACTOR_DEFINITION_VERSION.RELEASE_DATE,
          if (actorDefinitionVersion.releaseDate == null) {
            null
          } else {
            LocalDate.parse(actorDefinitionVersion.releaseDate)
          },
        ).set(
          Tables.ACTOR_DEFINITION_VERSION.ALLOWED_HOSTS,
          if (actorDefinitionVersion.allowedHosts == null) {
            null
          } else {
            JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.allowedHosts))
          },
        ).set(
          Tables.ACTOR_DEFINITION_VERSION.SUGGESTED_STREAMS,
          if (actorDefinitionVersion.suggestedStreams == null) {
            null
          } else {
            JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.suggestedStreams))
          },
        ).set(
          Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_REFRESHES,
          actorDefinitionVersion.supportsRefreshes != null && actorDefinitionVersion.supportsRefreshes,
        ).set(
          Tables.ACTOR_DEFINITION_VERSION.LAST_PUBLISHED,
          if (actorDefinitionVersion.lastPublished == null) {
            null
          } else {
            actorDefinitionVersion.lastPublished.toInstant().atOffset(ZoneOffset.UTC)
          },
        ).set(Tables.ACTOR_DEFINITION_VERSION.CDK_VERSION, actorDefinitionVersion.cdkVersion)
        .set(
          Tables.ACTOR_DEFINITION_VERSION.SUPPORT_STATE,
          actorDefinitionVersion.supportState.value().toEnum<SupportState>()!!,
        ).set(Tables.ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL, actorDefinitionVersion.internalSupportLevel)
        .set(Tables.ACTOR_DEFINITION_VERSION.LANGUAGE, actorDefinitionVersion.language)
        .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_FILE_TRANSFER, actorDefinitionVersion.supportsFileTransfer)
        .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_DATA_ACTIVATION, actorDefinitionVersion.supportsDataActivation)
        .set(
          Tables.ACTOR_DEFINITION_VERSION.CONNECTOR_IPC_OPTIONS,
          if (actorDefinitionVersion.connectorIPCOptions == null) {
            null
          } else {
            JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.connectorIPCOptions))
          },
        ).execute()
    }

    return actorDefinitionVersion
  }

  /**
   * Get the actor definition version associated with an actor definition and a docker image tag.
   *
   * @param actorDefinitionId - actor definition id
   * @param dockerImageTag - docker image tag
   * @param ctx database context
   * @return actor definition version if there is an entry in the DB already for this version,
   * otherwise an empty optional
   */
  fun getActorDefinitionVersion(
    actorDefinitionId: UUID?,
    dockerImageTag: String?,
    ctx: DSLContext,
  ): Optional<ActorDefinitionVersion> =
    ctx
      .selectFrom<ActorDefinitionVersionRecord>(Tables.ACTOR_DEFINITION_VERSION)
      .where(
        Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID
          .eq(actorDefinitionId)
          .and(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG.eq(dockerImageTag)),
      ).fetch()
      .stream()
      .findFirst()
      .map { obj: ActorDefinitionVersionRecord -> DbConverter.buildActorDefinitionVersion(obj) }

  /**
   * Get an optional ADV for an actor definition's default version. The optional will be empty if the
   * defaultVersionId of the actor definition is set to null in the DB. The only time this should be
   * the case is if we are in the process of inserting and have already written the actor definition,
   * but not yet set its default version.
   */
  fun getDefaultVersionForActorDefinitionIdOptional(
    actorDefinitionId: UUID?,
    ctx: DSLContext,
  ): Optional<ActorDefinitionVersion> =
    ctx
      .select(Tables.ACTOR_DEFINITION_VERSION.asterisk())
      .from(Tables.ACTOR_DEFINITION)
      .join(Tables.ACTOR_DEFINITION_VERSION)
      .on(Tables.ACTOR_DEFINITION_VERSION.ID.eq(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID))
      .where(Tables.ACTOR_DEFINITION.ID.eq(actorDefinitionId))
      .fetch()
      .stream()
      .findFirst()
      .map { obj: Record -> DbConverter.buildActorDefinitionVersion(obj) }

  /**
   * Writes a list of actor definition breaking changes in one transaction. Updates entries if they
   * already exist.
   *
   * @param breakingChanges - actor definition breaking changes to write
   * @param ctx database context
   */
  fun writeActorDefinitionBreakingChanges(
    breakingChanges: List<ActorDefinitionBreakingChange>,
    ctx: DSLContext,
  ) {
    val timestamp = OffsetDateTime.now()
    val upsertQueries =
      breakingChanges
        .stream()
        .map { breakingChange: ActorDefinitionBreakingChange -> upsertBreakingChangeQuery(ctx, breakingChange, timestamp) }
        .collect(Collectors.toList())
    ctx.batch(upsertQueries).execute()
  }

  private fun upsertBreakingChangeQuery(
    ctx: DSLContext,
    breakingChange: ActorDefinitionBreakingChange,
    timestamp: OffsetDateTime,
  ): Query =
    ctx
      .insertInto(Tables.ACTOR_DEFINITION_BREAKING_CHANGE)
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.ACTOR_DEFINITION_ID, breakingChange.actorDefinitionId)
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.VERSION, breakingChange.version.serialize())
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPGRADE_DEADLINE, LocalDate.parse(breakingChange.upgradeDeadline))
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MESSAGE, breakingChange.message)
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MIGRATION_DOCUMENTATION_URL, breakingChange.migrationDocumentationUrl)
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.SCOPED_IMPACT, JSONB.valueOf(Jsons.serialize(breakingChange.scopedImpact)))
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.DEADLINE_ACTION, breakingChange.deadlineAction)
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.CREATED_AT, timestamp)
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPDATED_AT, timestamp)
      .onConflict(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.ACTOR_DEFINITION_ID, Tables.ACTOR_DEFINITION_BREAKING_CHANGE.VERSION)
      .doUpdate()
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPGRADE_DEADLINE, LocalDate.parse(breakingChange.upgradeDeadline))
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MESSAGE, breakingChange.message)
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MIGRATION_DOCUMENTATION_URL, breakingChange.migrationDocumentationUrl)
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.SCOPED_IMPACT, JSONB.valueOf(Jsons.serialize(breakingChange.scopedImpact)))
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.DEADLINE_ACTION, breakingChange.deadlineAction)
      .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPDATED_AT, timestamp)
}
