/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.instance.configs.migrations.V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.ReleaseStage;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds the missing suggested_streams column to the actor_definition_version table. This migration
 * also fixes the type of the release_stage column to use the existing enum.
 */
public class V0_44_5_002__AddSuggestedStreamsToActorDefinitionVersion extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_44_5_002__AddSuggestedStreamsToActorDefinitionVersion.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addSuggestedStreams(ctx);
    fixReleaseStageType(ctx);
  }

  private static void fixReleaseStageType(final DSLContext ctx) {
    // Drops data, but is ok since the release_stage column had not been used yet.
    ctx.alterTable("actor_definition_version")
        .dropColumn("release_stage")
        .execute();

    ctx.alterTable("actor_definition_version")
        .addColumn(DSL.field("release_stage", SQLDataType.VARCHAR.asEnumDataType(ReleaseStage.class).nullable(true)))
        .execute();
  }

  private static void addSuggestedStreams(final DSLContext ctx) {
    ctx.alterTable("actor_definition_version")
        .addColumnIfNotExists(DSL.field(
            "suggested_streams",
            SQLDataType.JSONB.nullable(true)))
        .execute();
  }

}
