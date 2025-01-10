/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class V0_57_4_004__AddDeclarativeManifestImageVersionTableTest extends AbstractConfigsDatabaseTest {

  private static final String DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE = "declarative_manifest_image_version";

  private static final String MAJOR_VERSION = "major_version";
  private static final String IMAGE_VERSION = "image_version";
  private static final String CREATED_AT = "created_at";
  private static final String UPDATED_AT = "updated_at";

  @Test
  void test() throws SQLException, IOException {

    final DSLContext context = getDslContext();
    V0_57_4_004__AddDeclarativeManifestImageVersionTable.createDeclarativeManifestImageVersionTable(context);

    final Integer majorVersion = 0;
    final String declarativeManifestImageVersion = "0.0.1";
    final OffsetDateTime insertTime = OffsetDateTime.now();

    // assert can insert
    Assertions.assertDoesNotThrow(() -> {
      context.insertInto(DSL.table(DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE))
          .columns(
              DSL.field(MAJOR_VERSION),
              DSL.field(IMAGE_VERSION),
              DSL.field(CREATED_AT),
              DSL.field(UPDATED_AT))
          .values(
              majorVersion,
              declarativeManifestImageVersion,
              insertTime,
              insertTime)
          .execute();
    });

    // assert primary key is unique
    final Exception e = Assertions.assertThrows(DataAccessException.class, () -> {
      context.insertInto(DSL.table(DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE))
          .columns(
              DSL.field(MAJOR_VERSION),
              DSL.field(IMAGE_VERSION),
              DSL.field(CREATED_AT),
              DSL.field(UPDATED_AT))
          .values(
              majorVersion,
              declarativeManifestImageVersion,
              insertTime,
              insertTime)
          .execute();
    });

    Assertions.assertTrue(e.getMessage().contains("duplicate key value violates unique constraint \"declarative_manifest_image_version_pkey\""));
  }

}
