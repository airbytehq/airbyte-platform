/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * This migration drops the version-specific columns from the actor_definition table. These columns
 * have been previously moved to the actor_definition_version.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_4_002__DropActorDefinitionVersionedCols : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    ctx
      .alterTable("actor_definition")
      .dropColumns(
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
        "suggested_streams",
      ).execute()
  }
}
