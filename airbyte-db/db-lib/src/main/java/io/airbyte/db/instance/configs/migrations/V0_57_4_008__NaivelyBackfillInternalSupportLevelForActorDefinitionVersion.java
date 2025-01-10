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

/**
 * This is a migration to naively populate all actor_definition_version records with a
 * internal_support_level relative to their release stage. general_availability -> 300 and the rest
 * is the default value `100`
 */
public class V0_57_4_008__NaivelyBackfillInternalSupportLevelForActorDefinitionVersion extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      V0_57_4_008__NaivelyBackfillInternalSupportLevelForActorDefinitionVersion.class);

  static void backfillInternalSupportLevel(final DSLContext ctx) {
    ctx.transaction(configuration -> {
      final var transactionCtx = DSL.using(configuration);

      final var updateQuery =
          "UPDATE actor_definition_version SET internal_support_level = {0} WHERE release_stage = {1}";

      // The default value set in the previous migration is `100L` which maps to `community`. To keep the
      // previous behavior which is "we alert on `connector_release_stage == generally_available`", we
      // will simply migrate these to a support level of `300L`. On new version releases, these
      // value will eventually align properly with the actual values in the metadata files.
      transactionCtx.execute(updateQuery, 300L, ReleaseStage.generally_available);

      // Drop the default Internal Support Level
      transactionCtx.alterTable("actor_definition_version").alterColumn("internal_support_level").dropDefault().execute();
    });
  }

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    backfillInternalSupportLevel(ctx);
  }

  enum ReleaseStage implements EnumType {

    alpha("alpha"),
    beta("beta"),
    generally_available("generally_available"),
    custom("custom");

    private final String literal;

    ReleaseStage(final String literal) {
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
      return "release_stage";
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
