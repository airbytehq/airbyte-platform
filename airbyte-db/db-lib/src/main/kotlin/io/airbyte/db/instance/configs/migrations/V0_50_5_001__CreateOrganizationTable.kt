/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Insert new table for organization settings. Subsequent migrations will add an organization_id
 * column to the workspace table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_5_001__CreateOrganizationTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createOrganization(ctx)
  }

  companion object {
    @JvmStatic
    @VisibleForTesting
    fun createOrganization(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
      val userId = DSL.field("user_id", SQLDataType.UUID.nullable(true))
      val email = DSL.field("email", SQLDataType.VARCHAR(256).nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("organization")
        .columns(
          id,
          name,
          userId,
          email,
          createdAt,
          updatedAt,
        ).constraints(DSL.primaryKey(id))
        .execute()

      log.info { "organization table created" }
    }
  }
}
