/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_57_4_016__AddBaseActorDefVersionAndContributionColumnsToConnectorBuilderProject : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addBaseActorDefinitionVersionIdToConnectorBuilderProject(ctx)
    addContributionPullRequestUrlToConnectorBuilderProject(ctx)
    addContributionActorDefinitionIdToConnectorBuilderProject(ctx)
  }

  companion object {
    private const val CONNECTOR_BUILDER_PROJECT_TABLE = "connector_builder_project"

    private fun addBaseActorDefinitionVersionIdToConnectorBuilderProject(ctx: DSLContext) {
      val defaultVersionId = DSL.field("base_actor_definition_version_id", SQLDataType.UUID.nullable(true))

      ctx.alterTable(CONNECTOR_BUILDER_PROJECT_TABLE).addIfNotExists(defaultVersionId).execute()
      ctx
        .alterTable(CONNECTOR_BUILDER_PROJECT_TABLE)
        .add(
          DSL
            .constraint("connector_builder_project_base_adv_id_fkey")
            .foreignKey(defaultVersionId)
            .references("actor_definition_version", "id")
            .onDeleteRestrict(),
        ).execute()
    }

    private fun addContributionPullRequestUrlToConnectorBuilderProject(ctx: DSLContext) {
      val pullRequestUrl = DSL.field("contribution_pull_request_url", SQLDataType.VARCHAR(256).nullable(true))
      ctx.alterTable(CONNECTOR_BUILDER_PROJECT_TABLE).addIfNotExists(pullRequestUrl).execute()
    }

    private fun addContributionActorDefinitionIdToConnectorBuilderProject(ctx: DSLContext) {
      // This is not a foreign key because this is a reference to the actor definition that will be in the
      // catalog upon publish. In other words, the reference will resolve for forked connectors,
      // but will not resolve for new connectors until their publish is successful.
      val pullRequestUrl = DSL.field("contribution_actor_definition_id", SQLDataType.UUID.nullable(true))
      ctx.alterTable(CONNECTOR_BUILDER_PROJECT_TABLE).addIfNotExists(pullRequestUrl).execute()
    }
  }
}
