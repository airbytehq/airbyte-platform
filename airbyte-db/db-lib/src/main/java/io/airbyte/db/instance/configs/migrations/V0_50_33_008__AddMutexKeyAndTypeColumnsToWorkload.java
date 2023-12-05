/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jetbrains.annotations.NotNull;
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

public class V0_50_33_008__AddMutexKeyAndTypeColumnsToWorkload extends BaseJavaMigration {

  private static final String TABLE = "workload";
  private static final String MUTEX_KEY_COLUMN_NAME = "mutex_key";
  private static final String TYPE_COLUMN_NAME = "type";
  private static final String WORKLOAD_TYPE_ENUM_NAME = "workload_type";

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_008__AddMutexKeyAndTypeColumnsToWorkload.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    final Field<String> mutexKeyColumn = DSL.field(MUTEX_KEY_COLUMN_NAME, SQLDataType.VARCHAR.nullable(true));
    ctx.alterTable(TABLE)
        .addColumnIfNotExists(mutexKeyColumn)
        .execute();

    createWorkloadTypeEnum(ctx);

    final Field<WorkloadType> typeColumn = DSL.field(TYPE_COLUMN_NAME, SQLDataType.VARCHAR.asEnumDataType(WorkloadType.class).nullable(false));
    ctx.alterTable(TABLE)
        .addColumnIfNotExists(typeColumn)
        .execute();
  }

  private static void createWorkloadTypeEnum(final DSLContext ctx) {
    ctx.createType(WORKLOAD_TYPE_ENUM_NAME).asEnum(
        WorkloadType.SYNC.literal,
        WorkloadType.CHECK.literal,
        WorkloadType.DISCOVER.literal,
        WorkloadType.SPEC.literal).execute();
  }

  enum WorkloadType implements EnumType {

    SYNC("sync"),
    CHECK("check"),
    DISCOVER("discover"),
    SPEC("spec");

    private final String literal;

    WorkloadType(@NotNull final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return WORKLOAD_TYPE_ENUM_NAME;
    }

    @Override
    @NotNull
    public String getLiteral() {
      return literal;
    }

  }

}
