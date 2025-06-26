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

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_002__AllowNullSecretConfigUser : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx: DSLContext = DSL.using(context.connection)
    dropExtraForeignKeyConstraint(ctx)
    dropNotNullFromSecretConfigUserColumns(ctx)
  }

  companion object {
    private const val SECRET_CONFIG_TABLE_NAME = "secret_config"

    @VisibleForTesting
    fun dropExtraForeignKeyConstraint(ctx: DSLContext) {
      // This duplicate constraint was added in a previous migration.
      ctx
        .alterTable(SECRET_CONFIG_TABLE_NAME)
        .dropConstraintIfExists("secret_config_secret_storage_id_fkey1")
        .execute()
    }

    @VisibleForTesting
    fun dropNotNullFromSecretConfigUserColumns(ctx: DSLContext) {
      ctx
        .alterTable(SECRET_CONFIG_TABLE_NAME)
        .alterColumn("created_by")
        .dropNotNull()
        .execute()

      ctx
        .alterTable(SECRET_CONFIG_TABLE_NAME)
        .alterColumn("updated_by")
        .dropNotNull()
        .execute()
    }
  }
}
