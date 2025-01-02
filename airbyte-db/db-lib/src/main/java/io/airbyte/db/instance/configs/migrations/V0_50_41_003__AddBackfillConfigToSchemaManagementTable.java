/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.SCHEMA_MANAGEMENT_TABLE;

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
 * Modify the schema management table to support backfill preferences.
 */
public class V0_50_41_003__AddBackfillConfigToSchemaManagementTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_41_003__AddBackfillConfigToSchemaManagementTable.class);
  private static final String BACKFILL_PREFERENCE = "backfill_preference";
  private static final String ENABLED = "enabled";
  private static final String DISABLED = "disabled";

  enum BackfillPreference implements EnumType {

    enabled(ENABLED),
    disabled(DISABLED);

    private final String literal;

    BackfillPreference(final String literal) {
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
      return BACKFILL_PREFERENCE;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

  private static final Field<BackfillPreference> BACKFILL_PREFERENCE_COLUMN =
      DSL.field(BACKFILL_PREFERENCE, SQLDataType.VARCHAR.asEnumDataType(BackfillPreference.class)
          .nullable(false)
          .defaultValue(BackfillPreference.disabled));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addBackfillTypeEnum(ctx);
    addBackfillPreferenceColumnToSchemaManagementTable(ctx);
  }

  private static void addBackfillTypeEnum(DSLContext ctx) {
    ctx.createType(BACKFILL_PREFERENCE).asEnum(ENABLED, DISABLED).execute();
  }

  private static void addBackfillPreferenceColumnToSchemaManagementTable(DSLContext ctx) {
    ctx.alterTable(SCHEMA_MANAGEMENT_TABLE)
        .addIfNotExists(BACKFILL_PREFERENCE_COLUMN)
        .execute();
  }

}
