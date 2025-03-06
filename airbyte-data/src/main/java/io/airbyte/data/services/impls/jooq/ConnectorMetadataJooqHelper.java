/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION;

import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.db.instance.configs.jooq.generated.Tables;
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage;
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Query;

/**
 * Helper class used to read/write connector metadata from associated tables (breaking changes,
 * actor definition versions).
 */
public class ConnectorMetadataJooqHelper {

  /**
   * Write an actor definition version.
   *
   * @param actorDefinitionVersion - actor definition version to write
   * @param ctx database context
   * @return the POJO associated with the actor definition version written. Contains the versionId
   *         field from the DB.
   */
  public static ActorDefinitionVersion writeActorDefinitionVersion(final ActorDefinitionVersion actorDefinitionVersion, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();

    final Optional<UUID> providedVersionId = Optional.ofNullable(actorDefinitionVersion.getVersionId());

    final UUID actorDefinitionId = actorDefinitionVersion.getActorDefinitionId();
    final String dockerImageTag = actorDefinitionVersion.getDockerImageTag();
    final Optional<ActorDefinitionVersion> existingADV = getActorDefinitionVersion(actorDefinitionId, dockerImageTag, ctx);

    if (providedVersionId.isPresent() && existingADV.isPresent() && !existingADV.get().getVersionId().equals(providedVersionId.get())) {
      throw new RuntimeException(
          String.format("Provided version id %s does not match existing version id %s for actor definition %s and docker image tag %s",
              providedVersionId.get(), existingADV.get().getVersionId(), actorDefinitionId, dockerImageTag));
    }

    if (existingADV.isPresent()) {
      final UUID versionId = existingADV.get().getVersionId();
      actorDefinitionVersion.setVersionId(versionId);

      ctx.update(Tables.ACTOR_DEFINITION_VERSION)
          .set(ACTOR_DEFINITION_VERSION.UPDATED_AT, timestamp)
          .set(Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, actorDefinitionVersion.getDockerRepository())
          .set(Tables.ACTOR_DEFINITION_VERSION.SPEC, JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.getSpec())))
          .set(Tables.ACTOR_DEFINITION_VERSION.DOCUMENTATION_URL, actorDefinitionVersion.getDocumentationUrl())
          .set(Tables.ACTOR_DEFINITION_VERSION.PROTOCOL_VERSION, actorDefinitionVersion.getProtocolVersion())
          .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL, actorDefinitionVersion.getSupportLevel() == null ? null
              : Enums.toEnum(actorDefinitionVersion.getSupportLevel().value(),
                  SupportLevel.class).orElseThrow())
          .set(Tables.ACTOR_DEFINITION_VERSION.RELEASE_STAGE, actorDefinitionVersion.getReleaseStage() == null ? null
              : Enums.toEnum(actorDefinitionVersion.getReleaseStage().value(),
                  ReleaseStage.class).orElseThrow())
          .set(Tables.ACTOR_DEFINITION_VERSION.RELEASE_DATE, actorDefinitionVersion.getReleaseDate() == null ? null
              : LocalDate.parse(actorDefinitionVersion.getReleaseDate()))
          .set(Tables.ACTOR_DEFINITION_VERSION.ALLOWED_HOSTS, actorDefinitionVersion.getAllowedHosts() == null ? null
              : JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.getAllowedHosts())))
          .set(Tables.ACTOR_DEFINITION_VERSION.SUGGESTED_STREAMS,
              actorDefinitionVersion.getSuggestedStreams() == null ? null
                  : JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.getSuggestedStreams())))
          .set(ACTOR_DEFINITION_VERSION.SUPPORTS_REFRESHES,
              actorDefinitionVersion.getSupportsRefreshes() != null && actorDefinitionVersion.getSupportsRefreshes())
          .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORT_STATE,
              Enums.toEnum(actorDefinitionVersion.getSupportState().value(), io.airbyte.db.instance.configs.jooq.generated.enums.SupportState.class)
                  .orElseThrow())
          .set(ACTOR_DEFINITION_VERSION.LAST_PUBLISHED, actorDefinitionVersion.getLastPublished() == null ? null
              : actorDefinitionVersion.getLastPublished().toInstant().atOffset(ZoneOffset.UTC))
          .set(ACTOR_DEFINITION_VERSION.CDK_VERSION, actorDefinitionVersion.getCdkVersion())
          .set(ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL, actorDefinitionVersion.getInternalSupportLevel())
          .set(ACTOR_DEFINITION_VERSION.LANGUAGE, actorDefinitionVersion.getLanguage())
          .set(ACTOR_DEFINITION_VERSION.SUPPORTS_FILE_TRANSFER, actorDefinitionVersion.getSupportsFileTransfer())
          .where(ACTOR_DEFINITION_VERSION.ID.eq(versionId))
          .execute();
    } else {
      // If the version id is provided, use it (useful for mocks). Otherwise, generate a new one.
      final UUID versionId = providedVersionId.orElse(UUID.randomUUID());
      actorDefinitionVersion.setVersionId(versionId);

      ctx.insertInto(Tables.ACTOR_DEFINITION_VERSION)
          .set(Tables.ACTOR_DEFINITION_VERSION.ID, versionId)
          .set(ACTOR_DEFINITION_VERSION.CREATED_AT, timestamp)
          .set(ACTOR_DEFINITION_VERSION.UPDATED_AT, timestamp)
          .set(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID, actorDefinitionVersion.getActorDefinitionId())
          .set(Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, actorDefinitionVersion.getDockerRepository())
          .set(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, actorDefinitionVersion.getDockerImageTag())
          .set(Tables.ACTOR_DEFINITION_VERSION.SPEC, JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.getSpec())))
          .set(Tables.ACTOR_DEFINITION_VERSION.DOCUMENTATION_URL, actorDefinitionVersion.getDocumentationUrl())
          .set(Tables.ACTOR_DEFINITION_VERSION.PROTOCOL_VERSION, actorDefinitionVersion.getProtocolVersion())
          .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL, actorDefinitionVersion.getSupportLevel() == null ? null
              : Enums.toEnum(actorDefinitionVersion.getSupportLevel().value(),
                  SupportLevel.class).orElseThrow())
          .set(Tables.ACTOR_DEFINITION_VERSION.RELEASE_STAGE, actorDefinitionVersion.getReleaseStage() == null ? null
              : Enums.toEnum(actorDefinitionVersion.getReleaseStage().value(),
                  ReleaseStage.class).orElseThrow())
          .set(Tables.ACTOR_DEFINITION_VERSION.RELEASE_DATE, actorDefinitionVersion.getReleaseDate() == null ? null
              : LocalDate.parse(actorDefinitionVersion.getReleaseDate()))
          .set(Tables.ACTOR_DEFINITION_VERSION.ALLOWED_HOSTS, actorDefinitionVersion.getAllowedHosts() == null ? null
              : JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.getAllowedHosts())))
          .set(Tables.ACTOR_DEFINITION_VERSION.SUGGESTED_STREAMS,
              actorDefinitionVersion.getSuggestedStreams() == null ? null
                  : JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.getSuggestedStreams())))
          .set(ACTOR_DEFINITION_VERSION.SUPPORTS_REFRESHES,
              actorDefinitionVersion.getSupportsRefreshes() != null && actorDefinitionVersion.getSupportsRefreshes())
          .set(ACTOR_DEFINITION_VERSION.LAST_PUBLISHED, actorDefinitionVersion.getLastPublished() == null ? null
              : actorDefinitionVersion.getLastPublished().toInstant().atOffset(ZoneOffset.UTC))
          .set(ACTOR_DEFINITION_VERSION.CDK_VERSION, actorDefinitionVersion.getCdkVersion())
          .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORT_STATE,
              Enums.toEnum(actorDefinitionVersion.getSupportState().value(), io.airbyte.db.instance.configs.jooq.generated.enums.SupportState.class)
                  .orElseThrow())
          .set(ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL, actorDefinitionVersion.getInternalSupportLevel())
          .set(ACTOR_DEFINITION_VERSION.LANGUAGE, actorDefinitionVersion.getLanguage())
          .set(ACTOR_DEFINITION_VERSION.SUPPORTS_FILE_TRANSFER, actorDefinitionVersion.getSupportsFileTransfer())
          .execute();
    }

    return actorDefinitionVersion;
  }

  /**
   * Get the actor definition version associated with an actor definition and a docker image tag.
   *
   * @param actorDefinitionId - actor definition id
   * @param dockerImageTag - docker image tag
   * @param ctx database context
   * @return actor definition version if there is an entry in the DB already for this version,
   *         otherwise an empty optional
   */
  public static Optional<ActorDefinitionVersion> getActorDefinitionVersion(final UUID actorDefinitionId,
                                                                           final String dockerImageTag,
                                                                           final DSLContext ctx) {
    return ctx.selectFrom(Tables.ACTOR_DEFINITION_VERSION)
        .where(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID.eq(actorDefinitionId)
            .and(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG.eq(dockerImageTag)))
        .fetch()
        .stream()
        .findFirst()
        .map(DbConverter::buildActorDefinitionVersion);
  }

  /**
   * Get an optional ADV for an actor definition's default version. The optional will be empty if the
   * defaultVersionId of the actor definition is set to null in the DB. The only time this should be
   * the case is if we are in the process of inserting and have already written the actor definition,
   * but not yet set its default version.
   */
  public static Optional<ActorDefinitionVersion> getDefaultVersionForActorDefinitionIdOptional(final UUID actorDefinitionId, final DSLContext ctx) {
    return ctx.select(Tables.ACTOR_DEFINITION_VERSION.asterisk())
        .from(ACTOR_DEFINITION)
        .join(ACTOR_DEFINITION_VERSION).on(Tables.ACTOR_DEFINITION_VERSION.ID.eq(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID))
        .where(ACTOR_DEFINITION.ID.eq(actorDefinitionId))
        .fetch()
        .stream()
        .findFirst()
        .map(DbConverter::buildActorDefinitionVersion);
  }

  /**
   * Writes a list of actor definition breaking changes in one transaction. Updates entries if they
   * already exist.
   *
   * @param breakingChanges - actor definition breaking changes to write
   * @param ctx database context
   */
  public static void writeActorDefinitionBreakingChanges(final List<ActorDefinitionBreakingChange> breakingChanges, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final List<Query> upsertQueries = breakingChanges.stream()
        .map(breakingChange -> upsertBreakingChangeQuery(ctx, breakingChange, timestamp))
        .collect(Collectors.toList());
    ctx.batch(upsertQueries).execute();
  }

  private static Query upsertBreakingChangeQuery(final DSLContext ctx,
                                                 final ActorDefinitionBreakingChange breakingChange,
                                                 final OffsetDateTime timestamp) {
    return ctx.insertInto(Tables.ACTOR_DEFINITION_BREAKING_CHANGE)
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.ACTOR_DEFINITION_ID, breakingChange.getActorDefinitionId())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.VERSION, breakingChange.getVersion().serialize())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPGRADE_DEADLINE, LocalDate.parse(breakingChange.getUpgradeDeadline()))
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MESSAGE, breakingChange.getMessage())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MIGRATION_DOCUMENTATION_URL, breakingChange.getMigrationDocumentationUrl())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.SCOPED_IMPACT, JSONB.valueOf(Jsons.serialize(breakingChange.getScopedImpact())))
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.DEADLINE_ACTION, breakingChange.getDeadlineAction())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.CREATED_AT, timestamp)
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPDATED_AT, timestamp)
        .onConflict(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.ACTOR_DEFINITION_ID, Tables.ACTOR_DEFINITION_BREAKING_CHANGE.VERSION).doUpdate()
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPGRADE_DEADLINE, LocalDate.parse(breakingChange.getUpgradeDeadline()))
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MESSAGE, breakingChange.getMessage())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.MIGRATION_DOCUMENTATION_URL, breakingChange.getMigrationDocumentationUrl())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.SCOPED_IMPACT, JSONB.valueOf(Jsons.serialize(breakingChange.getScopedImpact())))
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.DEADLINE_ACTION, breakingChange.getDeadlineAction())
        .set(Tables.ACTOR_DEFINITION_BREAKING_CHANGE.UPDATED_AT, timestamp);
  }

}
