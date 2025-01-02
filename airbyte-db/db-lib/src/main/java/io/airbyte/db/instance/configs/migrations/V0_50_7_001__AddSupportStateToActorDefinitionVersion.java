/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

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
 * Adds support state to actor definition version. This migration does two things: 1. Adds a
 * support_state enum type. 2. Inserts a support_state column into the actor_definition_version
 * table. The support_state is a string that can be used to indicate whether an actor definition
 * version is supported, unsupported, or deprecated.
 */
public class V0_50_7_001__AddSupportStateToActorDefinitionVersion extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_7_001__AddSupportStateToActorDefinitionVersion.class);
  private static final String SUPPORT_STATE_TYPE_NAME = "support_state";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addSupportStateType(ctx);
    addSupportStateColumnToActorDefinitionVersion(ctx);
  }

  static void addSupportStateType(final DSLContext ctx) {
    ctx.dropTypeIfExists(SUPPORT_STATE_TYPE_NAME).execute();
    ctx.createType(SUPPORT_STATE_TYPE_NAME).asEnum("supported", "deprecated", "unsupported").execute();
  }

  static void addSupportStateColumnToActorDefinitionVersion(final DSLContext ctx) {

    final Field<SupportState> supportState =
        DSL.field("support_state", SQLDataType.VARCHAR.asEnumDataType(SupportState.class).nullable(false).defaultValue(SupportState.supported));
    ctx.alterTable("actor_definition_version").addColumnIfNotExists(supportState).execute();

    LOGGER.info("Added support_state column to actor_definition_version table and set to 'supported' for all existing rows");
  }

  enum SupportState implements EnumType {

    supported("supported"),
    deprecated("deprecated"),
    unsupported("unsupported");

    private final String literal;

    SupportState(final String literal) {
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
      return SUPPORT_STATE_TYPE_NAME;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
