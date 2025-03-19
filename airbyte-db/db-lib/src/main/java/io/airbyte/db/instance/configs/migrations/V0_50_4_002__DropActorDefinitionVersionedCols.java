/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This migration drops the version-specific columns from the actor_definition table. These columns
 * have been previously moved to the actor_definition_version.
 */
public class V0_50_4_002__DropActorDefinitionVersionedCols extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_4_002__DropActorDefinitionVersionedCols.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    ctx.alterTable("actor_definition").dropColumns(
        "docker_repository",
        "docker_image_tag",
        "documentation_url",
        "spec",
        "release_stage",
        "release_date",
        "protocol_version",
        "normalization_repository",
        "normalization_tag",
        "normalization_integration_type",
        "supports_dbt",
        "allowed_hosts",
        "suggested_streams").execute();
  }

}
