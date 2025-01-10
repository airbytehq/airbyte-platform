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

public class V0_50_41_009__AddBreakingChangeConfigOrigin extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_41_009__AddBreakingChangeConfigOrigin.class);
  private static final String CONFIG_ORIGIN_TYPE = "config_origin_type";
  private static final String BREAKING_CHANGE = "breaking_change";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    ctx.alterType(CONFIG_ORIGIN_TYPE).addValue(BREAKING_CHANGE).execute();
  }

  enum ConfigOriginType implements EnumType {

    USER("user"),
    BREAKING_CHANGE("breaking_change");

    private final String literal;

    ConfigOriginType(final String literal) {
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
      return CONFIG_ORIGIN_TYPE;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
