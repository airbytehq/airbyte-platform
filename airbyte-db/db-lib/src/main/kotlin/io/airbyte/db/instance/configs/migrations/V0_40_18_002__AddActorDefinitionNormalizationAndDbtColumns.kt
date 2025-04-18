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
 * Add actor definition normalization and dbt normalization columns migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_18_002__AddActorDefinitionNormalizationAndDbtColumns : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addNormalizationRepositoryColumn(ctx)
    addNormalizationTagColumn(ctx)
    addSupportsDbtColumn(ctx)
  }

  companion object {
    @JvmStatic
    fun addNormalizationRepositoryColumn(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field("normalization_repository", SQLDataType.VARCHAR(255).nullable(true)))
        .execute()
    }

    @JvmStatic
    fun addNormalizationTagColumn(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field("normalization_tag", SQLDataType.VARCHAR(255).nullable(true)))
        .execute()
    }

    @JvmStatic
    fun addSupportsDbtColumn(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field("supports_dbt", SQLDataType.BOOLEAN.nullable(true)))
        .execute()
    }
  }
}
