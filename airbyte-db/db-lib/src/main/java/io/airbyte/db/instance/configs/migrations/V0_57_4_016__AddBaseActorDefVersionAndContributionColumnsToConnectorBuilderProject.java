/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.constraint;

import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_57_4_016__AddBaseActorDefVersionAndContributionColumnsToConnectorBuilderProject extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      V0_57_4_016__AddBaseActorDefVersionAndContributionColumnsToConnectorBuilderProject.class);
  private static final String CONNECTOR_BUILDER_PROJECT_TABLE = "connector_builder_project";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    addBaseActorDefinitionVersionIdToConnectorBuilderProject(ctx);
    addContributionPullRequestUrlToConnectorBuilderProject(ctx);
    addContributionActorDefinitionIdToConnectorBuilderProject(ctx);
  }

  private static void addBaseActorDefinitionVersionIdToConnectorBuilderProject(final DSLContext ctx) {
    final Field<UUID> defaultVersionId = DSL.field("base_actor_definition_version_id", SQLDataType.UUID.nullable(true));

    ctx.alterTable(CONNECTOR_BUILDER_PROJECT_TABLE).addIfNotExists(defaultVersionId).execute();
    ctx.alterTable(CONNECTOR_BUILDER_PROJECT_TABLE)
        .add(constraint("connector_builder_project_base_adv_id_fkey").foreignKey(defaultVersionId)
            .references("actor_definition_version", "id").onDeleteRestrict())
        .execute();
  }

  private static void addContributionPullRequestUrlToConnectorBuilderProject(final DSLContext ctx) {
    final Field<String> pullRequestUrl = DSL.field("contribution_pull_request_url", SQLDataType.VARCHAR(256).nullable(true));
    ctx.alterTable(CONNECTOR_BUILDER_PROJECT_TABLE).addIfNotExists(pullRequestUrl).execute();
  }

  private static void addContributionActorDefinitionIdToConnectorBuilderProject(final DSLContext ctx) {
    // This is not a foreign key because this is a reference to the actor definition that will be in the
    // catalog upon publish. In other words, the reference will resolve for forked connectors,
    // but will not resolve for new connectors until their publish is successful.
    final Field<UUID> pullRequestUrl = DSL.field("contribution_actor_definition_id", SQLDataType.UUID.nullable(true));
    ctx.alterTable(CONNECTOR_BUILDER_PROJECT_TABLE).addIfNotExists(pullRequestUrl).execute();
  }

}
