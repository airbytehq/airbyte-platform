/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.CONNECTION_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.SCHEMA_MANAGEMENT_TABLE;
import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.Catalog;
import org.jooq.DSLContext;
import org.jooq.EnumType;
import org.jooq.Field;
import org.jooq.Schema;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migration to create a new table for schema management. For now, it only includes whether auto
 * propagation of schema changes is enabled. In the future, all of our schema management config will
 * be stored here.
 */
public class V0_44_3_001__AddSchemaManagementConfigurationExternalTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_44_3_001__AddSchemaManagementConfigurationExternalTable.class);
  private static final String AUTO_PROPAGATION_STATUS = "auto_propagation_status";
  private static final String ENABLED = "enabled";
  private static final String DISABLED = "disabled";

  enum AutoPropagationStatus implements EnumType {

    enabled(ENABLED),
    disabled(DISABLED);

    private final String literal;

    AutoPropagationStatus(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema() == null ? null : getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return "auto_propagation_status";
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

  private static final Field<UUID> ID_COLUMN = DSL.field("id", SQLDataType.UUID.nullable(false));
  private static final Field<AutoPropagationStatus> AUTO_PROPAGATION_STATUS_COLUMN =
      DSL.field("auto_propagation_status", SQLDataType.VARCHAR.asEnumDataType(
          AutoPropagationStatus.class).nullable(false));
  private static final Field<OffsetDateTime> CREATED_AT_COLUMN =
      DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
  private static final Field<OffsetDateTime> UPDATED_AT_COLUMN =
      DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
  private static final Field<UUID> CONNECTION_ID_COLUMN = DSL.field("connection_id", SQLDataType.UUID.nullable(false));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    addAutoPropagationTypeEnum(ctx);
    addSchemaManagementTable(ctx);
    addIndexOnConnectionId(ctx);
  }

  private static void addIndexOnConnectionId(final DSLContext ctx) {
    ctx.createIndexIfNotExists("connection_idx").on(SCHEMA_MANAGEMENT_TABLE, CONNECTION_ID_COLUMN.getName()).execute();
  }

  private static void addSchemaManagementTable(final DSLContext ctx) {
    ctx.createTableIfNotExists(SCHEMA_MANAGEMENT_TABLE)
        .columns(ID_COLUMN, AUTO_PROPAGATION_STATUS_COLUMN, CONNECTION_ID_COLUMN, CREATED_AT_COLUMN, UPDATED_AT_COLUMN)
        .constraints(primaryKey(ID_COLUMN), foreignKey(CONNECTION_ID_COLUMN).references(CONNECTION_TABLE, "id").onDeleteCascade())
        .execute();
  }

  private static void addAutoPropagationTypeEnum(final DSLContext ctx) {
    ctx.createType(AUTO_PROPAGATION_STATUS).asEnum(ENABLED, DISABLED).execute();
  }

}
