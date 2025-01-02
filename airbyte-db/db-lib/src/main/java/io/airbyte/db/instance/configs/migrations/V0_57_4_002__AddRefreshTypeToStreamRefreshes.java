/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import java.util.Arrays;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.Catalog;
import org.jooq.DSLContext;
import org.jooq.EnumType;
import org.jooq.Schema;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_57_4_002__AddRefreshTypeToStreamRefreshes extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_002__AddRefreshTypeToStreamRefreshes.class);
  private static final String STREAM_REFRESHES = "stream_refreshes";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    createRefreshTypeEnum(ctx);
    addRefreshTypeToStreamRefreshes(ctx);
    removeDefaultRefreshType(ctx);
  }

  private void createRefreshTypeEnum(final DSLContext ctx) {
    ctx.createType(RefreshType.NAME)
        .asEnum(Arrays.stream(RefreshType.values()).map(RefreshType::getLiteral).toArray(String[]::new))
        .execute();
  }

  private void addRefreshTypeToStreamRefreshes(final DSLContext ctx) {
    ctx.alterTable(STREAM_REFRESHES)
        .addColumnIfNotExists(
            DSL.field(RefreshType.NAME, SQLDataType.VARCHAR.asEnumDataType(RefreshType.class).defaultValue(RefreshType.TRUNCATE).nullable(false)))
        .execute();
  }

  private void removeDefaultRefreshType(final DSLContext ctx) {
    ctx.alterTable(STREAM_REFRESHES)
        .alterColumn(DSL.field(RefreshType.NAME))
        .dropDefault()
        .execute();
  }

  public enum RefreshType implements EnumType {

    MERGE("MERGE"),
    TRUNCATE("TRUNCATE");

    private final String literal;
    public static final String NAME = "refresh_type";

    RefreshType(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"));
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
