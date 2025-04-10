/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Configuration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import java.util.UUID

private val log = KotlinLogging.logger { }

/**
 * This is a migration to fix an issue where a catalog is un-traversable leading to a failing diff
 * when checking if the schema has breaking changes. The solution is to migrate anyone on the
 * invalid catalog to a new valid catalog Issue:
 * [...](https://github.com/airbytehq/oncall/issues/2703)
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_16_002__RemoveInvalidSourceStripeCatalog : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    removeInvalidSourceStripeCatalog(ctx)
  }

  companion object {
    const val VALID_CATALOG_CONTENT_HASH: String = "0c220b97"
    const val INVALID_CATALOG_CONTENT_HASH: String = "762fdbbf"

    @JvmStatic
    fun removeInvalidSourceStripeCatalog(ctx: DSLContext) {
      // get catalog id for invalid catalog (may not exist)
      val invalidCatalogIds = ctx.fetch("SELECT id FROM actor_catalog WHERE catalog_hash = {0}", INVALID_CATALOG_CONTENT_HASH)

      // get catalog id for valid catalog (may not exist)
      val validCatalogIds = ctx.fetch("SELECT id FROM actor_catalog WHERE catalog_hash = {0}", VALID_CATALOG_CONTENT_HASH)

      // if no invalid catalog or no valid catalog, do nothing
      if (invalidCatalogIds.size == 0 || validCatalogIds.size == 0) {
        log.info { "No invalid catalog or no valid catalog found. Skipping migration." }
        return
      }

      val invalidCatalogId = invalidCatalogIds[0].getValue("id", UUID::class.java)
      val validCatalogId = validCatalogIds[0].getValue("id", UUID::class.java)

      log.info { "Found invalid catalog id: $invalidCatalogId and valid catalog id: $validCatalogId" }

      // Transaction start
      ctx.transaction { configuration: Configuration? ->
        val transactionCtx = DSL.using(configuration)
        // For all connections with invalid catalog, update to valid catalog
        transactionCtx.execute(
          "UPDATE connection SET source_catalog_id = {0} WHERE source_catalog_id = {1}",
          validCatalogId,
          invalidCatalogId,
        )

        // Delete invalid catalog
        transactionCtx.execute("DELETE FROM actor_catalog WHERE id = {0}", invalidCatalogId)
      }
    }
  }
}
