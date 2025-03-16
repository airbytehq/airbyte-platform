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
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V1_1_1_016__AddDataplaneGroupIdToConnectionTest extends AbstractConfigsDatabaseTest {

  // DEFAULT_ORGANIZATION_ID is from io.airbyte.config.persistence.OrganizationPersistence
  public static final UUID DEFAULT_ORGANIZATION_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final Table<Record> CONNECTION = DSL.table("connection");
  private static final Table<Record> DATAPLANE_GROUP = DSL.table("dataplane_group");
  private static final Field<UUID> DATAPLANE_GROUP_ID = DSL.field("dataplane_group_id", SQLDataType.UUID);
  private static final Field<Object> GEOGRAPHY = DSL.field("geography", Object.class);
  private static final Field<UUID> ID = DSL.field("id", SQLDataType.UUID);
  private static final Field<String> NAME = DSL.field("name", SQLDataType.VARCHAR);
  private static final Field<OffsetDateTime> CREATED_AT = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE);
  private static final Field<OffsetDateTime> UPDATED_AT = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE);
  private static final Field<UUID> ORGANIZATION_ID = DSL.field("organization_id", SQLDataType.UUID);
  private static final Field<UUID> SOURCE_ID = DSL.field("source_id", SQLDataType.UUID);
  private static final Field<UUID> DESTINATION_ID = DSL.field("destination_id", SQLDataType.UUID);
  private static final Field<JSONB> CATALOG = DSL.field("catalog", SQLDataType.JSONB);
  private static final Field<Object> NAMESPACE_DEFINITION = DSL.field("namespace_definition", Object.class);
  private static final String NAMESPACE_DEFINITION_TYPE = "?::namespace_definition_type";
  private static final String GEOGRAPHY_TYPE = "?::geography_type";

  @BeforeEach
  void beforeEach() {
    final Flyway flyway = FlywayFactory.create(dataSource, "V1_1_1_016__AddDataplaneGroupIdToConnectionTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);

    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);
    final BaseJavaMigration previousMigration = new V1_1_1_015__AddAirbyteManagedBooleanToSecretConfigTable();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();

    final DSLContext ctx = getDslContext();
    dropConstraintsFromConnection(ctx);
    dropOrganizationIdFKFromDataplanegroup(ctx);
    dropUpdatedByFKFromDataplanegroup(ctx);
  }

  @Test
  void testDataplaneGroupIdIsPopulated() {
    final DSLContext ctx = getDslContext();
    UUID usDataplaneGroupId = UUID.randomUUID();
    UUID euDataplaneGroupId = UUID.randomUUID();
    UUID usConnectionId = UUID.randomUUID();
    UUID euConnectionId = UUID.randomUUID();
    UUID autoConnectionId = UUID.randomUUID();

    ctx.insertInto(DATAPLANE_GROUP, ID, ORGANIZATION_ID, NAME)
        .values(usDataplaneGroupId, DEFAULT_ORGANIZATION_ID, "US")
        .execute();
    ctx.insertInto(DATAPLANE_GROUP, ID, ORGANIZATION_ID, NAME)
        .values(euDataplaneGroupId, DEFAULT_ORGANIZATION_ID, "EU")
        .execute();

    ctx.insertInto(CONNECTION, ID, NAMESPACE_DEFINITION, SOURCE_ID, DESTINATION_ID, NAME, CATALOG, CREATED_AT, UPDATED_AT, GEOGRAPHY)
        .values(
            usConnectionId,
            DSL.field(NAMESPACE_DEFINITION_TYPE, NAMESPACE_DEFINITION.getDataType(), "source"),
            UUID.randomUUID(),
            UUID.randomUUID(),
            "test",
            JSONB.valueOf("{}"),
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.getDataType(), "US"))
        .execute();
    ctx.insertInto(CONNECTION, ID, NAMESPACE_DEFINITION, SOURCE_ID, DESTINATION_ID, NAME, CATALOG, CREATED_AT, UPDATED_AT, GEOGRAPHY)
        .values(
            euConnectionId,
            DSL.field(NAMESPACE_DEFINITION_TYPE, NAMESPACE_DEFINITION.getDataType(), "source"),
            UUID.randomUUID(),
            UUID.randomUUID(),
            "test",
            JSONB.valueOf("{}"),
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.getDataType(), "EU"))
        .execute();
    ctx.insertInto(CONNECTION, ID, NAMESPACE_DEFINITION, SOURCE_ID, DESTINATION_ID, NAME, CATALOG, CREATED_AT, UPDATED_AT, GEOGRAPHY)
        .values(
            autoConnectionId,
            DSL.field(NAMESPACE_DEFINITION_TYPE, NAMESPACE_DEFINITION.getDataType(), "source"),
            UUID.randomUUID(),
            UUID.randomUUID(),
            "test",
            JSONB.valueOf("{}"),
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            DSL.field(GEOGRAPHY_TYPE, GEOGRAPHY.getDataType(), Geography.AUTO.name()))
        .execute();

    V1_1_1_016__AddDataplaneGroupIdToConnection.doMigration(ctx);

    Map<UUID, UUID> connectionDataplaneMappings = ctx.select(ID, DATAPLANE_GROUP_ID)
        .from(CONNECTION)
        .fetchMap(r -> r.get(ID), r -> r.get(DATAPLANE_GROUP_ID));

    Assertions.assertEquals(usDataplaneGroupId, connectionDataplaneMappings.get(usConnectionId));
    Assertions.assertEquals(euDataplaneGroupId, connectionDataplaneMappings.get(euConnectionId));

    boolean isNullable = ctx.meta().getTables(CONNECTION.getName()).stream()
        .flatMap(Fields::fieldStream)
        .filter(field -> "dataplane_group_id".equals(field.getName()))
        .anyMatch(field -> field.getDataType().nullable());
    Assertions.assertFalse(isNullable);
  }

  private static void dropConstraintsFromConnection(final DSLContext ctx) {
    ctx.alterTable(CONNECTION)
        .dropConstraintIfExists("connection_source_id_fkey")
        .execute();
    ctx.alterTable(CONNECTION)
        .dropConstraintIfExists("connection_destination_id_fkey")
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
