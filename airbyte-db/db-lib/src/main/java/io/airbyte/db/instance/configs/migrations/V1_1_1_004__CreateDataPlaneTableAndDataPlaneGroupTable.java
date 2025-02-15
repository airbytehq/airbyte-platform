/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.unique;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_1_004__CreateDataPlaneTableAndDataPlaneGroupTable extends BaseJavaMigration {

  private static final String DATAPLANE_TABLE = "dataplane";
  private static final String DATAPLANE_GROUP_TABLE = "dataplane_group";
  private static final String USER_TABLE = "user";
  private static final String ORGANIZATION_TABLE = "organization";
  private static final String ID_FIELD_NAME = "id";
  private static final String NAME_FIELD_NAME = "name";
  private static final String DATAPLANE_GROUP_ID_FIELD_NAME = "dataplane_group_id";
  private static final String ORGANIZATION_ID_FIELD_NAME = "organization_id";
  private static final String ENABLED_FIELD_NAME = "enabled";
  private static final String CREATED_AT_FIELD_NAME = "created_at";
  private static final String UPDATED_AT_FIELD_NAME = "updated_at";
  private static final String UPDATED_BY_FIELD_NAME = "updated_by";
  private static final String TOMBSTONE_FIELD_NAME = "tombstone";

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_0_011__CreateConnectionTagTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    migrate(ctx);
  }

  public static void migrate(final DSLContext ctx) {
    createDataplaneGroupTable(ctx);
    createDataplaneTable(ctx);
  }

  private static void createDataplaneGroupTable(final DSLContext ctx) {
    final Field<UUID> id = DSL.field(ID_FIELD_NAME, SQLDataType.UUID.nullable(false));
    final Field<UUID> organizationId = DSL.field(ORGANIZATION_ID_FIELD_NAME, SQLDataType.UUID.nullable(false));
    final Field<String> name = DSL.field(NAME_FIELD_NAME, SQLDataType.VARCHAR(255).nullable(false));
    final Field<Boolean> enabled = DSL.field(ENABLED_FIELD_NAME, SQLDataType.BOOLEAN.nullable(false).defaultValue(true));
    final Field<OffsetDateTime> createdAt =
        DSL.field(CREATED_AT_FIELD_NAME, SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field(UPDATED_AT_FIELD_NAME, SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<UUID> updatedBy = DSL.field(UPDATED_BY_FIELD_NAME, SQLDataType.UUID.nullable(false));
    final Field<Boolean> tombstone = DSL.field(TOMBSTONE_FIELD_NAME, SQLDataType.BOOLEAN.nullable(false).defaultValue(false));

    ctx.createTable(DATAPLANE_GROUP_TABLE)
        .columns(id, organizationId, name, enabled, createdAt, updatedAt, updatedBy, tombstone)
        .constraints(
            primaryKey(id),
            foreignKey(organizationId).references(ORGANIZATION_TABLE, ID_FIELD_NAME).onDeleteCascade(),
            foreignKey(updatedBy).references(USER_TABLE, ID_FIELD_NAME).onDeleteCascade(),
            unique(organizationId, name))
        .execute();
  }

  private static void createDataplaneTable(final DSLContext ctx) {
    final Field<UUID> id = DSL.field(ID_FIELD_NAME, SQLDataType.UUID.nullable(false));
    final Field<UUID> dataplaneGroupId = DSL.field(DATAPLANE_GROUP_ID_FIELD_NAME, SQLDataType.UUID.nullable(false));
    final Field<String> name = DSL.field(NAME_FIELD_NAME, SQLDataType.VARCHAR(255).nullable(false));
    final Field<Boolean> enabled = DSL.field(ENABLED_FIELD_NAME, SQLDataType.BOOLEAN.nullable(false).defaultValue(true));
    final Field<OffsetDateTime> createdAt =
        DSL.field(CREATED_AT_FIELD_NAME, SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field(UPDATED_AT_FIELD_NAME, SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<UUID> updatedBy = DSL.field(UPDATED_BY_FIELD_NAME, SQLDataType.UUID.nullable(false));
    final Field<Boolean> tombstone = DSL.field(TOMBSTONE_FIELD_NAME, SQLDataType.BOOLEAN.nullable(false).defaultValue(false));

    ctx.createTable(DATAPLANE_TABLE)
        .columns(id, dataplaneGroupId, name, enabled, createdAt, updatedAt, updatedBy, tombstone)
        .constraints(
            primaryKey(id),
            foreignKey(dataplaneGroupId).references(DATAPLANE_GROUP_TABLE, ID_FIELD_NAME).onDeleteCascade(),
            foreignKey(updatedBy).references(USER_TABLE, ID_FIELD_NAME).onDeleteCascade(),
            unique(dataplaneGroupId, name))
        .execute();
  }

}
