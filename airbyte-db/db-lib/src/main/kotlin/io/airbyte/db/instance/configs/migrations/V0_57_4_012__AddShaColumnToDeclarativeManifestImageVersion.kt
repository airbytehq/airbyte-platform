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
class V0_57_4_012__AddShaColumnToDeclarativeManifestImageVersion : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    runMigration(ctx)
  }

  companion object {
    private const val DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE = "declarative_manifest_image_version"
    private val imageSha = DSL.field("image_sha", SQLDataType.VARCHAR(256).nullable(false))

    @JvmStatic
    fun runMigration(ctx: DSLContext) {
      clearDeclarativeManifestImageVersionTable(ctx)
      addShaToDeclarativeManifestImageVersionTable(ctx)
    }

    private fun clearDeclarativeManifestImageVersionTable(ctx: DSLContext) {
      // Clear entries in the table because they won't have SHAs.
      // These entries will be re-populated by the bootloader and then the following cron run.
      ctx.truncateTable(DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE).execute()
    }

    private fun addShaToDeclarativeManifestImageVersionTable(ctx: DSLContext) {
      ctx
        .alterTable(DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE)
        .addColumnIfNotExists(imageSha)
        .execute()
    }
  }
}
