/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.commons.constants.DataplaneConstantsKt;
import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class V1_1_1_013__PopulateDataplaneGroupsTest extends AbstractConfigsDatabaseTest {

  private static final Table<Record> WORKSPACE = DSL.table("workspace");
  private static final Table<Record> DATAPLANE_GROUP = DSL.table("dataplane_group");
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
    final Flyway flyway = FlywayFactory.create(dataSource, "V1_1_1_013__PopulateDataplaneGroupsTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);
    final BaseJavaMigration previousMigration = new V1_1_1_012__AddUniquenessConstraintToDataplaneClientCredentials();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();

    final DSLContext ctx = getDslContext();
    dropOrganizationIdFKFromWorkspace(ctx);
    dropOrganizationIdFKFromDataplanegroup(ctx);
    dropUpdatedByFKFromDataplanegroup(ctx);
  }

  @Test
  void testDataplaneGroupsAndDataplanesAreCreated() {
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
            DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.getDataType(), "US"),
            UUID.randomUUID())
        .execute();
    ctx.insertInto(WORKSPACE, ID, NAME, SLUG, INITIAL_SETUP_COMPLETE, TOMBSTONE, CREATED_AT, UPDATED_AT, GEOGRAPHY, ORGANIZATION_ID)
        .values(
            UUID.randomUUID(),
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
            UUID.randomUUID(),
            "",
            "",
            true,
            false,
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.getDataType(), DataplaneConstantsKt.GEOGRAPHY_AUTO),
            UUID.randomUUID())
        .execute();

    V1_1_1_013__PopulateDataplaneGroups.doMigration(ctx);

    Set<String> dataplaneGroupNames = ctx.select(NAME).from(DATAPLANE_GROUP).fetchSet(NAME);
    Assertions.assertTrue(dataplaneGroupNames.contains(DataplaneConstantsKt.GEOGRAPHY_US));
    Assertions.assertTrue(dataplaneGroupNames.contains(DataplaneConstantsKt.GEOGRAPHY_EU));
    Assertions.assertTrue(dataplaneGroupNames.contains(DataplaneConstantsKt.GEOGRAPHY_AUTO));
  }

  @Test
  void testDefaultDataplaneCreatedIfNoGeography() {
    final DSLContext ctx = getDslContext();

    V1_1_1_013__PopulateDataplaneGroups.doMigration(ctx);

    List<String> dataplaneNames = ctx.select(NAME).from(DATAPLANE_GROUP).fetch(NAME);
    Assertions.assertEquals(1, dataplaneNames.size());
    Assertions.assertEquals(DataplaneConstantsKt.GEOGRAPHY_AUTO, dataplaneNames.get(0));
  }

  @Test
  void testNoDuplicateDataplaneGroups() {
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
            DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.getDataType(), "US"),
            UUID.randomUUID())
        .execute();
    ctx.insertInto(WORKSPACE, ID, NAME, SLUG, INITIAL_SETUP_COMPLETE, TOMBSTONE, CREATED_AT, UPDATED_AT, GEOGRAPHY, ORGANIZATION_ID)
        .values(
            UUID.randomUUID(),
            "",
            "",
            true,
            false,
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.getDataType(), "US"),
            UUID.randomUUID())
        .execute();

    V1_1_1_013__PopulateDataplaneGroups.doMigration(ctx);

    long count = ctx.fetchCount(DATAPLANE_GROUP, NAME.eq("US"));
    Assertions.assertEquals(1, count);
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
