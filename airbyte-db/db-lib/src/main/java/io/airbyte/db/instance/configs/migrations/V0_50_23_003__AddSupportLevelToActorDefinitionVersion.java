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
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inserts a support_level column to the actor_definition_version table.
 */
public class V0_50_23_003__AddSupportLevelToActorDefinitionVersion extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_23_003__AddSupportLevelToActorDefinitionVersion.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    addSupportLevelToActorDefinitionVersion(ctx);
    LOGGER.info("support_level column added to actor_definition_version table");
  }

  static void addSupportLevelToActorDefinitionVersion(final DSLContext ctx) {
    createSupportLevelEnum(ctx);
    addSupportLevelColumn(ctx);
  }

  public static void createSupportLevelEnum(final DSLContext ctx) {
    ctx.createType("support_level").asEnum("community", "certified", "none").execute();
  }

  static void addSupportLevelColumn(final DSLContext ctx) {
    ctx.alterTable("actor_definition_version")
        .addColumnIfNotExists(DSL.field("support_level", SQLDataType.VARCHAR.asEnumDataType(
            V0_50_23_003__AddSupportLevelToActorDefinitionVersion.SupportLevel.class).nullable(false).defaultValue(SupportLevel.none)))
        .execute();
  }

  enum SupportLevel implements EnumType {

    community("community"),
    certified("certified"),
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
