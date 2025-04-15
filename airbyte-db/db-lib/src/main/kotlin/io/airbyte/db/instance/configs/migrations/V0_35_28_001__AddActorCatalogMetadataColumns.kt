/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.commons.annotation.InternalForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Add actor catalog metadata migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_28_001__AddActorCatalogMetadataColumns : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    migrate(ctx)
  }

  companion object {
    @JvmStatic
    @InternalForTesting
    fun migrate(ctx: DSLContext) {
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val modifiedAt = DSL.field("modified_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      ctx
        .alterTable("actor_catalog")
        .addIfNotExists(modifiedAt)
        .execute()
      ctx
        .alterTable("actor_catalog_fetch_event")
        .addIfNotExists(createdAt)
        .execute()
      ctx
        .alterTable("actor_catalog_fetch_event")
        .addIfNotExists(modifiedAt)
        .execute()
    }
  }
}
