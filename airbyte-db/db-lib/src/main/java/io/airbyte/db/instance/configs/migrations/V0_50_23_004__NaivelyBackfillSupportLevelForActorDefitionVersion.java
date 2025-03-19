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
 * This is a migration to naively populate all actor_definition_version records with a support_level
 * relative to their release stage. alpha -> community beta -> community general_availability ->
 * certified
 */
public class V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.class);

  static void backfillSupportLevel(final DSLContext ctx) {
    ctx.transaction(configuration -> {
      final var transactionCtx = DSL.using(configuration);

      final var updateQuery =
          "UPDATE actor_definition_version SET support_level = {0} WHERE release_stage = {1} AND support_level = 'none'::support_level";

      // For all connections with invalid catalog, update to valid catalog
      transactionCtx.execute(updateQuery, SupportLevel.community, ReleaseStage.alpha);
      transactionCtx.execute(updateQuery, SupportLevel.community, ReleaseStage.beta);
      transactionCtx.execute(updateQuery, SupportLevel.certified, ReleaseStage.generally_available);
      transactionCtx.execute(updateQuery, SupportLevel.none, ReleaseStage.custom);

      // Drop the default Support Level
      transactionCtx.alterTable("actor_definition_version").alterColumn("support_level").dropDefault().execute();
    });
  }

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    backfillSupportLevel(ctx);
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
