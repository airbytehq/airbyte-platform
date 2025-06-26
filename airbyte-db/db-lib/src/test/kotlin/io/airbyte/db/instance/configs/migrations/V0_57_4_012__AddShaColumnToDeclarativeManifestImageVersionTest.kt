/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_57_4_012__AddShaColumnToDeclarativeManifestImageVersionTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_57_4_011__DropUserTableAuthColumns",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V0_57_4_011__DropUserTableAuthColumns()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testExistingDataDoesNotBreakMigration() {
    val context = dslContext!!

    val majorVersion = 0
    val declarativeManifestImageVersion = "0.0.1"
    val insertTime = OffsetDateTime.now()

    // assert can insert
    Assertions.assertDoesNotThrow {
      context
        .insertInto(
          DSL.table(
            DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE,
          ),
        ).columns(
          DSL.field(MAJOR_VERSION),
          DSL.field(IMAGE_VERSION),
          DSL.field(CREATED_AT),
          DSL.field(UPDATED_AT),
        ).values(
          majorVersion,
          declarativeManifestImageVersion,
          insertTime,
          insertTime,
        ).execute()
    }

    V0_57_4_012__AddShaColumnToDeclarativeManifestImageVersion.runMigration(context)
  }

  companion object {
    private const val DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE = "declarative_manifest_image_version"

    private const val MAJOR_VERSION = "major_version"
    private const val IMAGE_VERSION = "image_version"
    private const val CREATED_AT = "created_at"
    private const val UPDATED_AT = "updated_at"
  }
}
