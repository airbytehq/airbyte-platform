/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.config.Geography;
import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Fields;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.IntegrityConstraintViolationException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class V1_1_1_014__AddDataplaneGroupIdToWorkspaceTest extends AbstractConfigsDatabaseTest {

  // DEFAULT_ORGANIZATION_ID is from io.airbyte.config.persistence.OrganizationPersistence
  public static final UUID DEFAULT_ORGANIZATION_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final Table<Record> WORKSPACE = DSL.table("workspace");
  private static final Table<Record> DATAPLANE_GROUP = DSL.table("dataplane_group");
  private static final Field<UUID> DATAPLANE_GROUP_ID = DSL.field("dataplane_group_id", SQLDataType.UUID);
  private static final Field<Object> GEOGRAPHY = DSL.field("geography", Object.class);
  private static final Field<UUID> ID = DSL.field("id", SQLDataType.UUID);
  private static final Field<String> NAME = DSL.field("name", SQLDataType.VARCHAR);
  private static final Field<String> SLUG = DSL.field("slug", SQLDataType.VARCHAR);
  private static final Field<Boolean> INITIAL_SETUP_COMPLETE = DSL.field("initial_setup_complete", SQLDataType.BOOLEAN);
  private static final Field<Boolean> TOMBSTONE = DSL.field("tombstone", SQLDataType.BOOLEAN);
  private static final Field<OffsetDateTime> CREATED_AT = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE);
  private static final Field<OffsetDateTime> UPDATED_AT = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE);
  private static final Field<UUID> ORGANIZATION_ID = DSL.field("organization_id", SQLDataType.UUID);
  private static final String GEOGRAPHY_TYPE = "?::geography_type";

  @BeforeEach
  void beforeEach() {
    final Flyway flyway = FlywayFactory.create(dataSource, "V1_1_1_014__AddDataplaneGroupIdToWorkspaceTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);

    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);
    final BaseJavaMigration previousMigration = new V1_1_1_013__PopulateDataplaneGroups();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();

    final DSLContext ctx = getDslContext();
    dropOrganizationIdFKFromWorkspace(ctx);
    dropOrganizationIdFKFromDataplanegroup(ctx);
    dropUpdatedByFKFromDataplanegroup(ctx);
  }

  @Test
  @Order(10)
  void testDataplaneGroupIdIsPopulated() {
    final DSLContext ctx = getDslContext();
    UUID usDataplaneGroupId = UUID.randomUUID();
    UUID euDataplaneGroupId = UUID.randomUUID();
    UUID usWorkspaceId = UUID.randomUUID();
    UUID euWorkspaceId = UUID.randomUUID();
    UUID autoWorkspaceId = UUID.randomUUID();

    ctx.insertInto(DATAPLANE_GROUP, ID, ORGANIZATION_ID, NAME)
        .values(usDataplaneGroupId, DEFAULT_ORGANIZATION_ID, "US")
        .execute();
    ctx.insertInto(DATAPLANE_GROUP, ID, ORGANIZATION_ID, NAME)
        .values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, "EU")
        .execute();

    ctx.insertInto(WORKSPACE, ID, NAME, SLUG, INITIAL_SETUP_COMPLETE, TOMBSTONE, CREATED_AT, UPDATED_AT, GEOGRAPHY, ORGANIZATION_ID)
        .values(
            usWorkspaceId,
            "",
            "",
            true,
            false,
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.getDataType(), "US"),
            UUID.randomUUID())
        .execute();
    ctx.insertInto(WORKSPACE, ID, NAME, SLUG, INITIAL_SETUP_COMPLETE, TOMBSTONE, CREATED_AT, UPDATED_AT, GEOGRAPHY, ORGANIZATION_ID)
        .values(
            euWorkspaceId,
            "",
            "",
            true,
            false,
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.getDataType(), "EU"),
            UUID.randomUUID())
        .execute();
    ctx.insertInto(WORKSPACE, ID, NAME, SLUG, INITIAL_SETUP_COMPLETE, TOMBSTONE, CREATED_AT, UPDATED_AT, GEOGRAPHY, ORGANIZATION_ID)
        .values(
            autoWorkspaceId,
            "",
            "",
            true,
            false,
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.getDataType(), Geography.AUTO.name()),
            UUID.randomUUID())
        .execute();

    V1_1_1_014__AddDataplaneGroupIdToWorkspace.doMigration(ctx);

    Map<UUID, UUID> workspaceDataplaneMappings = ctx.select(ID, DATAPLANE_GROUP_ID)
        .from(WORKSPACE)
        .fetchMap(r -> r.get(ID), r -> r.get(DATAPLANE_GROUP_ID));

    Assertions.assertEquals(usDataplaneGroupId, workspaceDataplaneMappings.get(usWorkspaceId));
    Assertions.assertEquals(euDataplaneGroupId, workspaceDataplaneMappings.get(euWorkspaceId));

    // AUTO is the default dataplane group and should always exist
    UUID autoDataplaneGroupId = ctx.select(ID)
        .from(DATAPLANE_GROUP)
        .where(NAME.eq("AUTO")).fetchOneInto(UUID.class);
    Assertions.assertEquals(autoDataplaneGroupId, workspaceDataplaneMappings.get(autoWorkspaceId));

    boolean isNullable = ctx.meta().getTables(WORKSPACE.getName()).stream()
        .flatMap(Fields::fieldStream)
        .filter(field -> "dataplane_group_id".equals(field.getName()))
        .anyMatch(field -> field.getDataType().nullable());
    Assertions.assertFalse(isNullable);

    boolean columnExists = ctx.meta().getTables(WORKSPACE.getName()).stream()
        .flatMap(Fields::fieldStream)
        .anyMatch(field -> "geography".equals(field.getName()));
    Assertions.assertFalse(columnExists);

    boolean deprecatedColumnExists = ctx.meta().getTables(WORKSPACE.getName()).stream()
        .flatMap(Fields::fieldStream)
        .anyMatch(field -> "geography_DO_NOT_USE".equals(field.getName()));
    Assertions.assertTrue(deprecatedColumnExists);
  }

  @Test
  @Order(1)
  void testMigrationThrowsWhenDataplaneGroupIdNotFoundForGeography() {
    final DSLContext ctx = getDslContext();
    ctx.insertInto(WORKSPACE, ID, NAME, SLUG, INITIAL_SETUP_COMPLETE, TOMBSTONE, CREATED_AT, UPDATED_AT, GEOGRAPHY, ORGANIZATION_ID)
        .values(
            UUID.randomUUID(),
            "",
            "",
            true,
            false,
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            // At this point only the dataplane group named "AUTO" exists.
            DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.getDataType(), "EU"),
            UUID.randomUUID())
        .execute();

    Assertions.assertThrows(IntegrityConstraintViolationException.class, () -> {
      V1_1_1_014__AddDataplaneGroupIdToWorkspace.doMigration(ctx);
    });

    boolean columnExists = ctx.meta().getTables(WORKSPACE.getName()).stream()
        .flatMap(Fields::fieldStream)
        .anyMatch(field -> "geography".equals(field.getName()));
    Assertions.assertTrue(columnExists);
  }

  @Test
  void testNameConstraintThrowsOnNotAllowedValues() {
    final DSLContext ctx = getDslContext();
    Assertions.assertThrows(DataAccessException.class, () -> ctx.insertInto(DATAPLANE_GROUP, ID, NAME)
        .values(UUID.randomUUID(), "INVALID")
        .execute());
  }

  private static void dropOrganizationIdFKFromWorkspace(final DSLContext ctx) {
    ctx.alterTable(WORKSPACE)
        .dropConstraintIfExists("workspace_organization_id_fkey")
        .execute();
  }

  private static void dropOrganizationIdFKFromDataplanegroup(final DSLContext ctx) {
    ctx.alterTable(DATAPLANE_GROUP)
        .dropConstraintIfExists("dataplane_group_organization_id_fkey")
        .execute();
  }

  private static void dropUpdatedByFKFromDataplanegroup(final DSLContext ctx) {
    ctx.alterTable(DATAPLANE_GROUP)
        .dropConstraintIfExists("dataplane_group_updated_by_fkey")
        .execute();
  }

}
