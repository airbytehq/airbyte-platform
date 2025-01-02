/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.version.Version;
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.ConfigResourceType;
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType;
import io.airbyte.db.instance.configs.migrations.V0_50_41_009__AddBreakingChangeConfigOrigin.ConfigOriginType;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_41_012__BreakingChangePinDataMigration extends BaseJavaMigration {

  private static final String CONNECTOR_VERSION_KEY = "connector_version";
  private static final Table<Record> ACTOR = DSL.table("actor");
  private static final Table<Record> ACTOR_DEFINITION = DSL.table("actor_definition");
  private static final Table<Record> ACTOR_DEFINITION_BREAKING_CHANGE = DSL.table("actor_definition_breaking_change");
  private static final Table<Record> WORKSPACE = DSL.table("workspace");
  private static final Table<Record> SCOPED_CONFIGURATION = DSL.table("scoped_configuration");
  private static final Field<UUID> ID = DSL.field("id", SQLDataType.UUID);
  private static final Field<String> KEY = DSL.field("key", SQLDataType.VARCHAR);
  private static final Field<ConfigResourceType> RESOURCE_TYPE = DSL.field("resource_type", ConfigResourceType.class);
  private static final Field<UUID> RESOURCE_ID = DSL.field("resource_id", SQLDataType.UUID);
  private static final Field<ConfigScopeType> SCOPE_TYPE = DSL.field("scope_type", ConfigScopeType.class);
  private static final Field<UUID> SCOPE_ID = DSL.field("scope_id", SQLDataType.UUID);
  private static final Field<String> VALUE = DSL.field("value", SQLDataType.VARCHAR);
  private static final Field<String> DESCRIPTION = DSL.field("description", SQLDataType.VARCHAR);
  private static final Field<ConfigOriginType> ORIGIN_TYPE = DSL.field("origin_type", ConfigOriginType.class);
  private static final Field<String> ORIGIN = DSL.field("origin", SQLDataType.VARCHAR);

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_41_012__BreakingChangePinDataMigration.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    migrateBreakingChangePins(ctx);
  }

  @VisibleForTesting
  public void migrateBreakingChangePins(final DSLContext ctx) {
    final List<ActorDefinition> actorDefinitions = getActorDefinitions(ctx);
    for (final ActorDefinition actorDefinition : actorDefinitions) {
      migrateBreakingChangePinsForDefinition(ctx, actorDefinition);
    }
  }

  private void migrateBreakingChangePinsForDefinition(final DSLContext ctx, final ActorDefinition actorDefinition) {
    final List<Actor> unpinnedActorsNotOnDefaultVersion = getUnpinnedActorsNotOnDefaultVersion(ctx, actorDefinition);
    final List<Version> breakingChangeVersions = getBreakingChangeVersionsForDefinition(ctx, actorDefinition.actorDefinitionId);
    for (final Actor actor : unpinnedActorsNotOnDefaultVersion) {
      final String originatingBreakingChange = getOriginatingBreakingChangeForVersion(ctx, actor.defaultVersionId, breakingChangeVersions);
      createScopedConfiguration(ctx, actorDefinition.actorDefinitionId, originatingBreakingChange, ConfigScopeType.ACTOR, actor.actorId,
          actor.defaultVersionId);
    }
  }

  private List<Actor> getUnpinnedActorsNotOnDefaultVersion(final DSLContext ctx, final ActorDefinition actorDefinition) {
    final List<Actor> actors = getActorsNotOnDefaultVersion(ctx, actorDefinition);
    final List<UUID> actorIdsWithConfig =
        getIdsWithConfig(ctx, actorDefinition.actorDefinitionId, ConfigScopeType.ACTOR, actors.stream().map(Actor::actorId).toList());
    final List<UUID> workspaceIdsWithConfig =
        getIdsWithConfig(ctx, actorDefinition.actorDefinitionId, ConfigScopeType.WORKSPACE, actors.stream().map(Actor::workspaceId).toList());
    final List<UUID> orgIdsWithConfig =
        getIdsWithConfig(ctx, actorDefinition.actorDefinitionId, ConfigScopeType.ORGANIZATION, actors.stream().map(Actor::organizationId).toList());

    return actors.stream()
        .filter(actor -> !actorIdsWithConfig.contains(actor.actorId()))
        .filter(actor -> !workspaceIdsWithConfig.contains(actor.workspaceId()))
        .filter(actor -> !orgIdsWithConfig.contains(actor.organizationId()))
        .toList();
  }

  final Map<UUID, String> versionBreakingChangeCache = new HashMap<>();

  private String getOriginatingBreakingChangeForVersion(final DSLContext ctx, final UUID versionId, final List<Version> breakingChangeVersions) {
    if (versionBreakingChangeCache.containsKey(versionId)) {
      return versionBreakingChangeCache.get(versionId);
    }

    final ActorDefinitionVersion version = getActorDefinitionVersion(ctx, versionId);
    final Version pinnedVersion = new Version(version.dockerImageTag);

    final Optional<Version> breakingVersion = breakingChangeVersions.stream()
        .filter(breakingChangeVersion -> breakingChangeVersion.greaterThan(pinnedVersion))
        .findFirst();

    if (breakingVersion.isEmpty()) {
      throw new IllegalStateException(String.format(
          "Could not find a corresponding breaking change for pinned version %s on actor definition ID %s. "
              + "Overriding actor versions without a breaking change is not supported.",
          version.dockerImageTag, version.actorDefinitionId));
    }

    final String originatingBreakingChange = breakingVersion.get().serialize();
    versionBreakingChangeCache.put(versionId, originatingBreakingChange);
    return originatingBreakingChange;
  }

  private List<UUID> getIdsWithConfig(final DSLContext ctx,
                                      final UUID actorDefinitionId,
                                      final ConfigScopeType scopeType,
                                      final List<UUID> scopeIds) {
    return ctx.select(SCOPE_ID)
        .from(SCOPED_CONFIGURATION)
        .where(KEY.eq(CONNECTOR_VERSION_KEY))
        .and(RESOURCE_TYPE.eq(ConfigResourceType.ACTOR_DEFINITION))
        .and(RESOURCE_ID.eq(actorDefinitionId))
        .and(SCOPE_TYPE.eq(scopeType))
        .and(SCOPE_ID.in(scopeIds))
        .fetch()
        .map(r -> r.get(SCOPE_ID));
  }

  private void createScopedConfiguration(final DSLContext ctx,
                                         final UUID actorDefinitionId,
                                         final String breakingChangeVersionTag,
                                         final ConfigScopeType scopeType,
                                         final UUID scopeId,
                                         final UUID pinnedVersionId) {
    ctx.insertInto(SCOPED_CONFIGURATION)
        .columns(ID, KEY, RESOURCE_TYPE, RESOURCE_ID, SCOPE_TYPE, SCOPE_ID, ORIGIN_TYPE, ORIGIN, VALUE, DESCRIPTION)
        .values(
            UUID.randomUUID(),
            CONNECTOR_VERSION_KEY,
            ConfigResourceType.ACTOR_DEFINITION, actorDefinitionId,
            scopeType, scopeId,
            ConfigOriginType.BREAKING_CHANGE, breakingChangeVersionTag,
            pinnedVersionId.toString(),
            "Automated breaking change pin migration")
        .execute();
  }

  private List<ActorDefinition> getActorDefinitions(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID);
    final Field<UUID> defaultVersionId = DSL.field("default_version_id", SQLDataType.UUID);

    return ctx.select(id, defaultVersionId)
        .from(ACTOR_DEFINITION)
        .fetch()
        .map(r -> new ActorDefinition(r.get(id), r.get(defaultVersionId)));
  }

  private ActorDefinitionVersion getActorDefinitionVersion(final DSLContext ctx, final UUID versionId) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID);
    final Field<UUID> actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID);
    final Field<String> dockerImageTag = DSL.field("docker_image_tag", SQLDataType.VARCHAR);

    return ctx.select(id, actorDefinitionId, dockerImageTag)
        .from("actor_definition_version")
        .where(id.eq(versionId))
        .fetchOne(r -> new ActorDefinitionVersion(r.get(id), r.get(actorDefinitionId), r.get(dockerImageTag)));
  }

  private List<Version> getBreakingChangeVersionsForDefinition(final DSLContext ctx, final UUID actorDefinitionId) {
    final Field<UUID> actorDefinitionIdField = DSL.field("actor_definition_id", SQLDataType.UUID);
    final Field<String> version = DSL.field("version", SQLDataType.VARCHAR);

    return ctx.select(version)
        .from(ACTOR_DEFINITION_BREAKING_CHANGE)
        .where(actorDefinitionIdField.eq(actorDefinitionId))
        .fetch()
        .map(r -> new Version(r.get(version)))
        .stream().sorted(Version::versionCompareTo)
        .toList();
  }

  private List<Actor> getActorsNotOnDefaultVersion(final DSLContext ctx, final ActorDefinition actorDefinition) {
    // Actor fields
    final Field<UUID> actorId = DSL.field("actor.id", SQLDataType.UUID);
    final Field<UUID> actorWorkspaceId = DSL.field("actor.workspace_id", SQLDataType.UUID);
    final Field<UUID> actorDefaultVersionId = DSL.field("actor.default_version_id", SQLDataType.UUID);
    final Field<UUID> actorDefinitionId = DSL.field("actor.actor_definition_id", SQLDataType.UUID);

    // Workspace fields
    final Field<UUID> workspaceId = DSL.field("workspace.id", SQLDataType.UUID);
    final Field<UUID> workspaceOrgId = DSL.field("workspace.organization_id", SQLDataType.UUID);

    return ctx.select(actorId, actorWorkspaceId, workspaceOrgId, actorDefaultVersionId)
        .from(ACTOR)
        .join(WORKSPACE).on(workspaceId.eq(actorWorkspaceId))
        .where(actorDefinitionId.eq(actorDefinition.actorDefinitionId))
        .and(actorDefaultVersionId.ne(actorDefinition.defaultVersionId))
        .fetch()
        .map(r -> new Actor(r.get(actorId), r.get(actorWorkspaceId), r.get(workspaceOrgId), r.get(actorDefaultVersionId)));

  }

  record ActorDefinition(UUID actorDefinitionId, UUID defaultVersionId) {}

  record ActorDefinitionVersion(UUID versionId, UUID actorDefinitionId, String dockerImageTag) {}

  record Actor(UUID actorId, UUID workspaceId, @Nullable UUID organizationId, UUID defaultVersionId) {}

}
