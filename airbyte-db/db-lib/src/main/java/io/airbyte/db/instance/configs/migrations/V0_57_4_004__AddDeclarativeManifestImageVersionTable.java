/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.primaryKey;

import java.time.OffsetDateTime;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_57_4_004__AddDeclarativeManifestImageVersionTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_004__AddDeclarativeManifestImageVersionTable.class);

  private static final String DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE = "declarative_manifest_image_version";

  private static final Field<Integer> majorVersion = DSL.field("major_version", SQLDataType.INTEGER.nullable(false));
  private static final Field<String> imageVersion = DSL.field("image_version", SQLDataType.VARCHAR(256).nullable(false));

  private static final Field<OffsetDateTime> createdAtField =
      DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
  private static final Field<OffsetDateTime> updatedAtField =
      DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createDeclarativeManifestImageVersionTable(ctx);
  }

  static void createDeclarativeManifestImageVersionTable(final DSLContext ctx) {

    ctx.createTable(DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE)
        .columns(majorVersion, imageVersion, createdAtField, updatedAtField)
        .constraints(primaryKey(majorVersion))
        .execute();
  }

}
