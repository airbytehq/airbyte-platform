/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a migration to fix an issue where a catalog is un-traversable leading to a failing diff
 * when checking if the schema has breaking changes. The solution is to migrate anyone on the
 * invalid catalog to a new valid catalog Issue:
 * <a href="https://github.com/airbytehq/oncall/issues/2703">...</a>
 */
@SuppressWarnings("PMD.UseCollectionIsEmpty")
public class V0_50_16_002__RemoveInvalidSourceStripeCatalog extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      V0_50_16_002__RemoveInvalidSourceStripeCatalog.class);

  public static final String VALID_CATALOG_CONTENT_HASH = "0c220b97";
  public static final String INVALID_CATALOG_CONTENT_HASH = "762fdbbf";

  static void removeInvalidSourceStripeCatalog(final DSLContext ctx) {

    // get catalog id for invalid catalog (may not exist)
    final var invalidCatalogIds = ctx.fetch("SELECT id FROM actor_catalog WHERE catalog_hash = {0}", INVALID_CATALOG_CONTENT_HASH);

    // get catalog id for valid catalog (may not exist)
    final var validCatalogIds = ctx.fetch("SELECT id FROM actor_catalog WHERE catalog_hash = {0}", VALID_CATALOG_CONTENT_HASH);

    // if no invalid catalog or no valid catalog, do nothing
    if (invalidCatalogIds.size() == 0 || validCatalogIds.size() == 0) {
      LOGGER.info("No invalid catalog or no valid catalog found. Skipping migration.");
      return;
    }

    final var invalidCatalogId = invalidCatalogIds.get(0).getValue("id", UUID.class);
    final var validCatalogId = validCatalogIds.get(0).getValue("id", UUID.class);

    LOGGER.info("Found invalid catalog id: {} and valid catalog id: {}", invalidCatalogId, validCatalogId);

    // Transaction start
    ctx.transaction(configuration -> {
      final var transactionCtx = DSL.using(configuration);

      // For all connections with invalid catalog, update to valid catalog
      transactionCtx.execute("UPDATE connection SET source_catalog_id = {0} WHERE source_catalog_id = {1}", validCatalogId, invalidCatalogId);

      // Delete invalid catalog
      transactionCtx.execute("DELETE FROM actor_catalog WHERE id = {0}", invalidCatalogId);
    });
  }

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    removeInvalidSourceStripeCatalog(ctx);
  }

}
