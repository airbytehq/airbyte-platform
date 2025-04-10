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
 * Add service account to workspace migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_65_001__CreateWorkspaceServiceAccountTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)
    createAndPopulateWorkspace(ctx)
  }

  companion object {
    private fun createAndPopulateWorkspace(ctx: DSLContext) {
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false))
      val serviceAccountId = DSL.field("service_account_id", SQLDataType.VARCHAR(31).nullable(false))
      val serviceAccountEmail = DSL.field("service_account_email", SQLDataType.VARCHAR(256).nullable(false))
      val jsonCredential = DSL.field("json_credential", SQLDataType.JSONB.nullable(false))
      val hmacKey = DSL.field("hmac_key", SQLDataType.JSONB.nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("workspace_service_account")
        .columns(
          workspaceId,
          serviceAccountId,
          serviceAccountEmail,
          jsonCredential,
          hmacKey,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(workspaceId, serviceAccountId),
          DSL.foreignKey(workspaceId).references("workspace", "id").onDeleteCascade(),
        ).execute()
      log.info { "workspace_service_account table created" }
    }
  }
}
