/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.time.OffsetDateTime;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V0_57_4_012__AddShaColumnToDeclarativeManifestImageVersionTest extends AbstractConfigsDatabaseTest {

  private static final String DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE = "declarative_manifest_image_version";

  private static final String MAJOR_VERSION = "major_version";
  private static final String IMAGE_VERSION = "image_version";
  private static final String CREATED_AT = "created_at";
  private static final String UPDATED_AT = "updated_at";

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_57_4_011__DropUserTableAuthColumns", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_57_4_011__DropUserTableAuthColumns();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void testExistingDataDoesNotBreakMigration() {

    final DSLContext context = getDslContext();

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

    V0_57_4_012__AddShaColumnToDeclarativeManifestImageVersion.runMigration(context);
  }

}
