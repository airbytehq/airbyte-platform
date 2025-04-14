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
class V0_57_4_006__AddCdkVersionLastModifiedToActorDefVersion : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    addCdkVersionToActorDefinitionVersion(ctx)
    addLastPublishedToActorDefinitionVersion(ctx)
    addMetricsToActorDefinition(ctx)
  }

  companion object {
    fun addCdkVersionToActorDefinitionVersion(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition_version")
        .addColumnIfNotExists(DSL.field("cdk_version", SQLDataType.VARCHAR(256).nullable(true)))
        .execute()
    }

    fun addLastPublishedToActorDefinitionVersion(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition_version")
        .addColumnIfNotExists(DSL.field("last_published", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true)))
        .execute()
    }

    fun addMetricsToActorDefinition(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field("metrics", SQLDataType.JSONB.nullable(true)))
        .execute()
    }
  }
}
