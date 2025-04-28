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
class V1_1_1_017__AddOrganizationIdToActorOauthParameter : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    doMigration(ctx)
  }

  companion object {
    private val ORGANIZATION_ID_COLUMN = DSL.field("organization_id", SQLDataType.UUID.nullable(true))
    private val WORKSPACE_ID_COLUMN = DSL.field("workspace_id", SQLDataType.UUID.nullable(true))
    const val ACTOR_OAUTH_PARAMETER: String = "actor_oauth_parameter"
    const val ONLY_WORKSPACE_OR_ORG_IS_SET: String = "only_workspace_or_org_is_set"

    @JvmStatic
    fun doMigration(ctx: DSLContext) {
      ctx
        .alterTable(ACTOR_OAUTH_PARAMETER)
        .addColumnIfNotExists(ORGANIZATION_ID_COLUMN, SQLDataType.UUID.nullable(true))
        .execute()

      ctx
        .alterTable(ACTOR_OAUTH_PARAMETER)
        .add(
          DSL.constraint(ONLY_WORKSPACE_OR_ORG_IS_SET).check(
            ORGANIZATION_ID_COLUMN.isNull().or(
              WORKSPACE_ID_COLUMN.isNull(),
            ),
          ),
        ).execute()
    }
  }
}
