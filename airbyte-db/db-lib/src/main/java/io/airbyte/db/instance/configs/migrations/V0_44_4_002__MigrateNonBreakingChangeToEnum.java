/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.CONNECTION_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.SCHEMA_MANAGEMENT_TABLE;
import static org.jooq.impl.DSL.currentOffsetDateTime;

import io.airbyte.config.StandardSync;
import java.time.OffsetDateTime;
import java.util.List;
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

/**
 * This migration is the first step to get rid of the non_breaking_schema_change_preference in the
 * connection table. The issue with this column is that it got create as a varchar instead of an
 * enum, this makes it hard to add value. This migration deletes the auto_propagation_status column
 * in the schema_management table as well as the type of it, then recreate the enum with the right
 * value and re-add the column. Finally, it creates entries in the schema_management table for all
 * the connections with a disable preference for the non_breaking_schema_change.
 */
public class V0_44_4_002__MigrateNonBreakingChangeToEnum extends BaseJavaMigration {

  private static final String AUTO_PROPAGATION_STATUS = "auto_propagation_status";

  private static final Field<V0_44_3_001__AddSchemaManagementConfigurationExternalTable.AutoPropagationStatus> OLD_AUTO_PROPAGATION_STATUS_COLUMN =
      DSL.field(AUTO_PROPAGATION_STATUS, SQLDataType.VARCHAR.asEnumDataType(
          V0_44_3_001__AddSchemaManagementConfigurationExternalTable.AutoPropagationStatus.class).nullable(false));

  private static final Field<AutoPropagationStatus> NEW_AUTO_PROPAGATION_STATUS_COLUMN =
      DSL.field(AUTO_PROPAGATION_STATUS, SQLDataType.VARCHAR.asEnumDataType(
          AutoPropagationStatus.class).nullable(false).defaultValue(AutoPropagationStatus.ignore));

  private static final Field<UUID> ID_COLUMN = DSL.field("id", SQLDataType.UUID);
  private static final Field<UUID> CONNECTION_ID_COLUMN = DSL.field("connection_id", SQLDataType.UUID);
  private static final Field<OffsetDateTime> CREATED_AT_COLUMN =
      DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE);
  private static final Field<OffsetDateTime> UPDATE_AT_COLUMN =
      DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE);

  @Override
  public void migrate(final Context context) throws Exception {
    final DSLContext ctx = DSL.using(context.getConnection());

    dropExistingColumnAndType(ctx);
    recreateEnumType(ctx);
    addColumnWithUpdateType(ctx);
    migrateExistingDisableConnection(ctx);
  }

  /**
   * Drop the column in the schema_management table since the 2 enum values are not being used. This
   * will be recreated later, using the values from the connection table
   */
  private static void dropExistingColumnAndType(final DSLContext ctx) {
    ctx.alterTable(SCHEMA_MANAGEMENT_TABLE).dropColumn(OLD_AUTO_PROPAGATION_STATUS_COLUMN).execute();
    ctx.dropTypeIfExists(AUTO_PROPAGATION_STATUS).execute();
  }

  /**
   * Recreate the enum type using the config Enum values.
   */
  private static void recreateEnumType(final DSLContext ctx) {
    ctx.createType(AUTO_PROPAGATION_STATUS).asEnum(StandardSync.NonBreakingChangesPreference.DISABLE.value(),
        StandardSync.NonBreakingChangesPreference.IGNORE.value(),
        StandardSync.NonBreakingChangesPreference.PROPAGATE_COLUMNS.value(),
        StandardSync.NonBreakingChangesPreference.PROPAGATE_FULLY.value()).execute();
  }

  /**
   * Add the column to the schema management table.
   */
  private static void addColumnWithUpdateType(final DSLContext ctx) {
    ctx.alterTable(SCHEMA_MANAGEMENT_TABLE).addColumn(NEW_AUTO_PROPAGATION_STATUS_COLUMN).execute();
  }

  /**
   * Migrate the connection that are currently disable. At the time this PR is being created, there is
   * 29 connections with the non_breaking_change_preference set to disable.
   */
  private static void migrateExistingDisableConnection(final DSLContext ctx) {
    final List<UUID> disabledConnectionIds = ctx.select(ID_COLUMN)
        .from(CONNECTION_TABLE)
        .where(DSL.field("non_breaking_change_preference")
            .eq(DSL.cast("disable",
                SQLDataType.VARCHAR.asEnumDataType(V0_40_11_002__AddSchemaChangeColumnsToConnections.NonBreakingChangePreferenceType.class))))
        .stream()
        .map(record -> record.getValue(ID_COLUMN))
        .toList();

    disabledConnectionIds.forEach(connectionId -> {
      ctx.insertInto(DSL.table(SCHEMA_MANAGEMENT_TABLE))
          .set(ID_COLUMN, UUID.randomUUID())
          .set(CONNECTION_ID_COLUMN, connectionId)
          .set(NEW_AUTO_PROPAGATION_STATUS_COLUMN, AutoPropagationStatus.disabled)
          .set(CREATED_AT_COLUMN, currentOffsetDateTime())
          .set(UPDATE_AT_COLUMN, currentOffsetDateTime())
          .execute();
    });
  }

  enum AutoPropagationStatus implements EnumType {

    disabled(StandardSync.NonBreakingChangesPreference.DISABLE.value()),
    ignore(StandardSync.NonBreakingChangesPreference.IGNORE.value()),
    propagate_column(StandardSync.NonBreakingChangesPreference.PROPAGATE_COLUMNS.value()),
    propagate_fully(StandardSync.NonBreakingChangesPreference.PROPAGATE_FULLY.value());

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

}
