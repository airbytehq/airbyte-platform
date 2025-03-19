/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.Catalog;
import org.jooq.DSLContext;
import org.jooq.EnumType;
import org.jooq.Schema;
import org.jooq.impl.DSL;
import org.jooq.impl.SchemaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_41_006__AlterSupportLevelAddArchived extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_41_006__AlterSupportLevelAddArchived.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    updateSupportLevelEnum(ctx);
    LOGGER.info("support_level enum updated");
  }

  static void updateSupportLevelEnum(final DSLContext ctx) {
    ctx.alterType("support_level").addValue("archived").execute();
  }

  enum SupportLevel implements EnumType {

    community("community"),
    certified("certified"),
    archived("archived"),
    none("none");

    private final String literal;

    SupportLevel(final String literal) {
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
      return "support_level";
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
