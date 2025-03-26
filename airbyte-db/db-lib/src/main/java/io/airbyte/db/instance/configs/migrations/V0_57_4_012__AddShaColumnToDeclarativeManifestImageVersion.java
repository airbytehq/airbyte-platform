/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_57_4_012__AddShaColumnToDeclarativeManifestImageVersion extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_012__AddShaColumnToDeclarativeManifestImageVersion.class);
  private static final String DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE = "declarative_manifest_image_version";
  private static final Field<String> imageSha = DSL.field("image_sha", SQLDataType.VARCHAR(256).nullable(false));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    runMigration(ctx);
  }

  static void runMigration(final DSLContext ctx) {
    clearDeclarativeManifestImageVersionTable(ctx);
    addShaToDeclarativeManifestImageVersionTable(ctx);
  }

  private static void clearDeclarativeManifestImageVersionTable(final DSLContext ctx) {
    // Clear entries in the table because they won't have SHAs.
    // These entries will be re-populated by the bootloader and then the following cron run.
    ctx.truncateTable(DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE).execute();
  }

  private static void addShaToDeclarativeManifestImageVersionTable(final DSLContext ctx) {
    ctx.alterTable(DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE)
        .addColumnIfNotExists(imageSha)
        .execute();
  }

}
