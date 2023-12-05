/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION;

import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.db.instance.configs.jooq.generated.Tables;
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage;
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel;
import java.io.IOException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.JSONB;

/**
 * Helper class to keep shared jooq logic used in actors (source, destination). This should be
 * removed in favor of a combined Actor service that deals with both sources and destinations
 * uniformly.
 */
public class ActorDefinitionVersionJooqHelper {

  /**
   * Write an actor definition version.
   *
   * @param actorDefinitionVersion - actor definition version to write
   * @param ctx database context
   * @throws IOException - you never know when you io
   * @returns the POJO associated with the actor definition version written. Contains the versionId
   *          field from the DB.
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
          .set(Tables.ACTOR_DEFINITION_VERSION.NORMALIZATION_REPOSITORY,
              Objects.nonNull(actorDefinitionVersion.getNormalizationConfig())
                  ? actorDefinitionVersion.getNormalizationConfig().getNormalizationRepository()
                  : null)
          .set(Tables.ACTOR_DEFINITION_VERSION.NORMALIZATION_TAG,
              Objects.nonNull(actorDefinitionVersion.getNormalizationConfig())
                  ? actorDefinitionVersion.getNormalizationConfig().getNormalizationTag()
                  : null)
          .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_DBT, actorDefinitionVersion.getSupportsDbt())
          .set(Tables.ACTOR_DEFINITION_VERSION.NORMALIZATION_INTEGRATION_TYPE,
              Objects.nonNull(actorDefinitionVersion.getNormalizationConfig())
                  ? actorDefinitionVersion.getNormalizationConfig().getNormalizationIntegrationType()
                  : null)
          .set(Tables.ACTOR_DEFINITION_VERSION.ALLOWED_HOSTS, actorDefinitionVersion.getAllowedHosts() == null ? null
              : JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.getAllowedHosts())))
          .set(Tables.ACTOR_DEFINITION_VERSION.SUGGESTED_STREAMS,
              actorDefinitionVersion.getSuggestedStreams() == null ? null
                  : JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.getSuggestedStreams())))
          .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORT_STATE,
              Enums.toEnum(actorDefinitionVersion.getSupportState().value(), io.airbyte.db.instance.configs.jooq.generated.enums.SupportState.class)
                  .orElseThrow())
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
          .set(Tables.ACTOR_DEFINITION_VERSION.NORMALIZATION_REPOSITORY,
              Objects.nonNull(actorDefinitionVersion.getNormalizationConfig())
                  ? actorDefinitionVersion.getNormalizationConfig().getNormalizationRepository()
                  : null)
          .set(Tables.ACTOR_DEFINITION_VERSION.NORMALIZATION_TAG,
              Objects.nonNull(actorDefinitionVersion.getNormalizationConfig())
                  ? actorDefinitionVersion.getNormalizationConfig().getNormalizationTag()
                  : null)
          .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORTS_DBT, actorDefinitionVersion.getSupportsDbt())
          .set(Tables.ACTOR_DEFINITION_VERSION.NORMALIZATION_INTEGRATION_TYPE,
              Objects.nonNull(actorDefinitionVersion.getNormalizationConfig())
                  ? actorDefinitionVersion.getNormalizationConfig().getNormalizationIntegrationType()
                  : null)
          .set(Tables.ACTOR_DEFINITION_VERSION.ALLOWED_HOSTS, actorDefinitionVersion.getAllowedHosts() == null ? null
              : JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.getAllowedHosts())))
          .set(Tables.ACTOR_DEFINITION_VERSION.SUGGESTED_STREAMS,
              actorDefinitionVersion.getSuggestedStreams() == null ? null
                  : JSONB.valueOf(Jsons.serialize(actorDefinitionVersion.getSuggestedStreams())))
          .set(Tables.ACTOR_DEFINITION_VERSION.SUPPORT_STATE,
              Enums.toEnum(actorDefinitionVersion.getSupportState().value(), io.airbyte.db.instance.configs.jooq.generated.enums.SupportState.class)
                  .orElseThrow())
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
   * @throws IOException - you never know when you io
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
   * Set the ActorDefinitionVersion for a given tag as the default version for the associated actor
   * definition. Check docker image tag on the new ADV; if an ADV exists for that tag, set the
   * existing ADV for the tag as the default. Otherwise, insert the new ADV and set it as the default.
   *
   * @param actorDefinitionVersion new actor definition version
   * @throws IOException - you never know when you IO
   */
  public static void setActorDefinitionVersionForTagAsDefault(final ActorDefinitionVersion actorDefinitionVersion,
                                                              final List<ActorDefinitionBreakingChange> breakingChangesForDefinition,
                                                              final DSLContext ctx) {
    final ActorDefinitionVersion writtenADV = writeActorDefinitionVersion(actorDefinitionVersion, ctx);
    setActorDefinitionVersionAsDefaultVersion(writtenADV, breakingChangesForDefinition, ctx);
  }

  private static void setActorDefinitionVersionAsDefaultVersion(final ActorDefinitionVersion actorDefinitionVersion,
                                                                final List<ActorDefinitionBreakingChange> breakingChangesForDefinition,
                                                                final DSLContext ctx) {
    if (actorDefinitionVersion.getVersionId() == null) {
      throw new RuntimeException("Can't set an actorDefinitionVersion as default without it having a versionId.");
    }

    final Optional<ActorDefinitionVersion> currentDefaultVersion =
        getDefaultVersionForActorDefinitionIdOptional(actorDefinitionVersion.getActorDefinitionId(), ctx);

    currentDefaultVersion
        .ifPresent(currentDefault -> {
          final boolean shouldUpdateActorDefaultVersions = shouldUpdateActorsDefaultVersionsDuringUpgrade(
              currentDefault.getDockerImageTag(), actorDefinitionVersion.getDockerImageTag(), breakingChangesForDefinition);
          if (shouldUpdateActorDefaultVersions) {
            updateDefaultVersionIdForActorsOnVersion(currentDefault.getVersionId(), actorDefinitionVersion.getVersionId(), ctx);
          }
        });

    updateActorDefinitionDefaultVersionId(actorDefinitionVersion.getActorDefinitionId(), actorDefinitionVersion.getVersionId(), ctx);
  }

  /**
   * Get an optional ADV for an actor definition's default version. The optional will be empty if the
   * defaultVersionId of the actor definition is set to null in the DB. The only time this should be
   * the case is if we are in the process of inserting and have already written the source definition,
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

  private static void updateDefaultVersionIdForActorsOnVersion(final UUID previousDefaultVersionId,
                                                               final UUID newDefaultVersionId,
                                                               final DSLContext ctx) {
    ctx.update(ACTOR)
        .set(ACTOR.UPDATED_AT, OffsetDateTime.now())
        .set(ACTOR.DEFAULT_VERSION_ID, newDefaultVersionId)
        .where(ACTOR.DEFAULT_VERSION_ID.eq(previousDefaultVersionId))
        .execute();
  }

  /**
   * Given a current version and a version to upgrade to, and a list of breaking changes, determine
   * whether actors' default versions should be updated during upgrade. This logic is used to avoid
   * applying a breaking change to a user's actor.
   *
   * @param currentDockerImageTag version to upgrade from
   * @param dockerImageTagForUpgrade version to upgrade to
   * @param breakingChangesForDef a list of breaking changes to check
   * @return whether actors' default versions should be updated during upgrade
   */
  private static boolean shouldUpdateActorsDefaultVersionsDuringUpgrade(final String currentDockerImageTag,
                                                                        final String dockerImageTagForUpgrade,
                                                                        final List<ActorDefinitionBreakingChange> breakingChangesForDef) {
    if (breakingChangesForDef.isEmpty()) {
      // If there aren't breaking changes, early exit in order to avoid trying to parse versions.
      // This is helpful for custom connectors or local dev images for connectors that don't have
      // breaking changes.
      return true;
    }

    final Version currentVersion = new Version(currentDockerImageTag);
    final Version versionToUpgradeTo = new Version(dockerImageTagForUpgrade);

    if (versionToUpgradeTo.lessThanOrEqualTo(currentVersion)) {
      // When downgrading, we don't take into account breaking changes/hold actors back.
      return true;
    }

    final boolean upgradingOverABreakingChange = breakingChangesForDef.stream().anyMatch(
        breakingChange -> currentVersion.lessThan(breakingChange.getVersion()) && versionToUpgradeTo.greaterThanOrEqualTo(
            breakingChange.getVersion()));
    return !upgradingOverABreakingChange;
  }

  private static void updateActorDefinitionDefaultVersionId(final UUID actorDefinitionId, final UUID versionId, final DSLContext ctx) {
    ctx.update(ACTOR_DEFINITION)
        .set(ACTOR_DEFINITION.UPDATED_AT, OffsetDateTime.now())
        .set(ACTOR_DEFINITION.DEFAULT_VERSION_ID, versionId)
        .where(ACTOR_DEFINITION.ID.eq(actorDefinitionId))
        .execute();
  }

}
