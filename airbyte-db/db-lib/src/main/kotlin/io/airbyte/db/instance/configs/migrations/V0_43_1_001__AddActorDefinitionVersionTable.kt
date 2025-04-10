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

/**
 * Add actor_definition_version table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_43_1_001__AddActorDefinitionVersionTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createActorDefinitionVersionTable(ctx)
    ctx
      .createIndexIfNotExists("actor_definition_version_definition_image_tag_idx")
      .on("actor_definition_version", "actor_definition_id", "docker_image_tag")
      .execute()
  }

  companion object {
    private fun createActorDefinitionVersionTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val documentationUrl = DSL.field("documentation_url", SQLDataType.VARCHAR(256).nullable(true))
      val dockerRepository = DSL.field("docker_repository", SQLDataType.VARCHAR(256).nullable(false))
      val dockerImageTag = DSL.field("docker_image_tag", SQLDataType.VARCHAR(256).nullable(false))
      val spec = DSL.field("spec", SQLDataType.JSONB.nullable(false))
      val protocolVersion = DSL.field("protocol_version", SQLDataType.VARCHAR(256).nullable(true))
      val releaseStage = DSL.field("release_stage", SQLDataType.VARCHAR(256).nullable(true))
      val releaseDate = DSL.field("release_date", SQLDataType.DATE.nullable(true))
      val normalizationRepository = DSL.field("normalization_repository", SQLDataType.VARCHAR(256).nullable(true))
      val normalizationTag = DSL.field("normalization_tag", SQLDataType.VARCHAR(256).nullable(true))
      val supportsDbt = DSL.field("supports_dbt", SQLDataType.BOOLEAN.nullable(true))
      val normalizationIntegrationType =
        DSL.field("normalization_integration_type", SQLDataType.VARCHAR(256).nullable(true))
      val allowedHosts = DSL.field("allowed_hosts", SQLDataType.JSONB.nullable(true))

      ctx
        .createTableIfNotExists("actor_definition_version")
        .columns(
          id,
          actorDefinitionId,
          createdAt,
          updatedAt,
          documentationUrl,
          dockerRepository,
          dockerImageTag,
          spec,
          protocolVersion,
          releaseStage,
          releaseDate,
          normalizationRepository,
          normalizationTag,
          supportsDbt,
          normalizationIntegrationType,
          allowedHosts,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(actorDefinitionId).references("actor_definition", "id").onDeleteCascade(),
        ).execute()
    }
  }
}
