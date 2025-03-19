/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType;
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.ConfigResourceType;
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType;
import io.airbyte.db.instance.configs.migrations.V0_50_41_006__AlterSupportLevelAddArchived.SupportLevel;
import io.airbyte.db.instance.configs.migrations.V0_50_41_009__AddBreakingChangeConfigOrigin.ConfigOriginType;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings({"PMD.AvoidDuplicateLiterals", "PMD.JUnitTestsShouldIncludeAssert"})
class V0_50_41_012__BreakingChangePinDataMigrationTest extends AbstractConfigsDatabaseTest {

  private V0_50_41_012__BreakingChangePinDataMigration migration;

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_41_012__BreakingChangePinDataMigrationTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_41_009__AddBreakingChangeConfigOrigin();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();

    migration = new V0_50_41_012__BreakingChangePinDataMigration();
  }

  static Stream<Arguments> testMethodSource() {
    return Stream.of(
        // Already on latest (3.1.0), no BC pin
        Arguments.of("3.1.0", List.of(), null),

        // Held back on an older version should create pin with correct BC as origin
        Arguments.of("0.1.0", List.of(), "1.0.0"),
        Arguments.of("1.0.0", List.of(), "2.0.0"),

        // Actors already pinned (at any level) should be ignored
        Arguments.of("1.0.0", List.of(ConfigScopeType.ACTOR), null),
        Arguments.of("1.0.0", List.of(ConfigScopeType.WORKSPACE), null),
        Arguments.of("1.0.0", List.of(ConfigScopeType.ORGANIZATION), null),
        Arguments.of("1.0.0", List.of(ConfigScopeType.ACTOR, ConfigScopeType.WORKSPACE, ConfigScopeType.ORGANIZATION), null));
  }

  @ParameterizedTest
  @MethodSource("testMethodSource")
  void testBreakingChangeOriginScopedConfig(final String actorVersion,
                                            final List<ConfigScopeType> existingConfigScopes,
                                            @Nullable final String expectedBCOrigin) {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    final UUID actorDefinitionId = UUID.randomUUID();
    createActorDefinition(ctx, actorDefinitionId);

    final UUID defaultVersionId = UUID.randomUUID();
    final String defaultVersionTag = "3.1.0";
    createActorDefinitionVersion(ctx, defaultVersionId, actorDefinitionId, defaultVersionTag);
    setActorDefinitionDefaultVersion(ctx, actorDefinitionId, defaultVersionId);

    UUID actorVersionId = defaultVersionId;
    if (!actorVersion.equals(defaultVersionTag)) {
      actorVersionId = UUID.randomUUID();
      createActorDefinitionVersion(ctx, actorVersionId, actorDefinitionId, actorVersion);
    }

    final UUID workspaceId = UUID.randomUUID();
    final UUID organizationId = UUID.randomUUID();
    createWorkspace(ctx, workspaceId, organizationId);

    final UUID actorId = UUID.randomUUID();
    createActor(ctx, actorId, workspaceId, actorDefinitionId, actorVersionId);

    for (final ConfigScopeType existingConfigScope : existingConfigScopes) {
      final UUID scopeId;
      switch (existingConfigScope) {
        case ACTOR -> scopeId = actorId;
        case WORKSPACE -> scopeId = workspaceId;
        case ORGANIZATION -> scopeId = organizationId;
        default -> throw new IllegalArgumentException("Unexpected config scope type: " + existingConfigScope);
      }
      createScopedConfig(ctx, actorDefinitionId, existingConfigScope, scopeId, ConfigOriginType.USER, "userId", actorVersion);
    }

    final List<String> breakingChanges = List.of("1.0.0", "2.0.0", "3.0.0");
    for (final String breakingChange : breakingChanges) {
      createBreakingChange(ctx, actorDefinitionId, breakingChange);
    }

    // run migration
    migration.migrateBreakingChangePins(ctx);

    // get pin and assert it's correct
    final Optional<Map<String, String>> scopedConfig = getScopedConfig(ctx, actorDefinitionId, actorId);
    if (expectedBCOrigin == null) {
      assert (scopedConfig.isEmpty());
    } else {
      assert (scopedConfig.isPresent());
      assert (scopedConfig.get().get("value").equals(actorVersionId.toString()));
      assert (scopedConfig.get().get("origin").equals(expectedBCOrigin));
    }

  }

  private static void createActorDefinition(final DSLContext ctx, final UUID actorDefinitionId) {
    ctx.insertInto(DSL.table("actor_definition"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("actor_type"))
        .values(
            actorDefinitionId,
            "postgres",
            ActorType.source)
        .execute();
  }

  private static void setActorDefinitionDefaultVersion(final DSLContext ctx, final UUID actorDefinitionId, final UUID defaultVersionId) {
    ctx.update(DSL.table("actor_definition"))
        .set(DSL.field("default_version_id"), defaultVersionId)
        .where(DSL.field("id").eq(actorDefinitionId))
        .execute();
  }

  private static void createActorDefinitionVersion(final DSLContext ctx,
                                                   final UUID actorDefinitionVersionId,
                                                   final UUID actorDefinitionId,
                                                   final String version) {
    ctx.insertInto(DSL.table("actor_definition_version"))
        .columns(
            DSL.field("id"),
            DSL.field("actor_definition_id"),
            DSL.field("docker_repository"),
            DSL.field("docker_image_tag"),
            DSL.field("support_level"),
            DSL.field("spec", SQLDataType.JSONB))
        .values(
            actorDefinitionVersionId,
            actorDefinitionId,
            "airbyte/postgres",
            version,
            SupportLevel.community,
            JSONB.valueOf("{}"))
        .execute();
  }

  private static void createActor(final DSLContext ctx,
                                  final UUID actorId,
                                  final UUID workspaceId,
                                  final UUID actorDefinitionId,
                                  final UUID defaultVersionId) {
    ctx.insertInto(DSL.table("actor"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("actor_type"),
            DSL.field("workspace_id"),
            DSL.field("actor_definition_id"),
            DSL.field("default_version_id"),
            DSL.field("configuration", SQLDataType.JSONB))
        .values(
            actorId,
            "postgres",
            ActorType.source,
            workspaceId,
            actorDefinitionId,
            defaultVersionId,
            JSONB.valueOf("{}"))
        .execute();
  }

  private static void createWorkspace(final DSLContext ctx, final UUID workspaceId, final UUID organizationId) {
    ctx.insertInto(DSL.table("workspace"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"),
            DSL.field("organization_id"))
        .values(
            workspaceId,
            "workspace",
            "workspace",
            true,
            organizationId)
        .execute();
  }

  private static void createScopedConfig(
                                         final DSLContext ctx,
                                         final UUID actorDefinitionId,
                                         final ConfigScopeType scopeType,
                                         final UUID scopeId,
                                         final ConfigOriginType originType,
                                         final String origin,
                                         final String value) {
    ctx.insertInto(DSL.table("scoped_configuration"))
        .columns(
            DSL.field("id"),
            DSL.field("key"),
            DSL.field("resource_type"),
            DSL.field("resource_id"),
            DSL.field("scope_type"),
            DSL.field("scope_id"),
            DSL.field("value"),
            DSL.field("origin_type"),
            DSL.field("origin"))
        .values(
            UUID.randomUUID(),
            "connector_version",
            ConfigResourceType.ACTOR_DEFINITION,
            actorDefinitionId,
            scopeType,
            scopeId,
            value,
            originType,
            origin)
        .execute();
  }

  private static Optional<Map<String, String>> getScopedConfig(final DSLContext ctx, final UUID actorDefinitionId, final UUID scopeId) {
    return ctx.select(DSL.field("value"), DSL.field("origin"))
        .from(DSL.table("scoped_configuration"))
        .where(DSL.field("resource_type").eq(ConfigResourceType.ACTOR_DEFINITION)
            .and(DSL.field("resource_id").eq(actorDefinitionId))
            .and(DSL.field("scope_type").eq(ConfigScopeType.ACTOR))
            .and(DSL.field("scope_id").eq(scopeId))
            .and(DSL.field("origin_type").eq(ConfigOriginType.BREAKING_CHANGE)))
        .fetchOptional()
        .map(r -> r
            .map(record -> Map.of(
                "value", record.get(DSL.field("value", String.class)),
                "origin", record.get(DSL.field("origin", String.class)))));
  }

  private static void createBreakingChange(final DSLContext ctx, final UUID actorDefinitionId, final String version) {
    ctx.insertInto(DSL.table("actor_definition_breaking_change"))
        .columns(
            DSL.field("actor_definition_id"),
            DSL.field("version"),
            DSL.field("migration_documentation_url"),
            DSL.field("message"),
            DSL.field("upgrade_deadline", SQLDataType.DATE))
        .values(
            actorDefinitionId,
            version,
            "https://docs.airbyte.io/",
            "Breaking change",
            Date.valueOf(LocalDate.now()))
        .execute();
  }

}
