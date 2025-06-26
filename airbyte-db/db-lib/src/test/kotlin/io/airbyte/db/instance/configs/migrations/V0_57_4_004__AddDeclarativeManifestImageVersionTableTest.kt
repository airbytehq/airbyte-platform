/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.migrations.V0_57_4_004__AddDeclarativeManifestImageVersionTable.Companion.createDeclarativeManifestImageVersionTable
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.IOException
import java.sql.SQLException
import java.time.OffsetDateTime

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_57_4_004__AddDeclarativeManifestImageVersionTableTest : AbstractConfigsDatabaseTest() {
  @Test
  @Throws(SQLException::class, IOException::class)
  fun test() {
    val context = dslContext!!
    createDeclarativeManifestImageVersionTable(context)

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

    // assert primary key is unique
    val e: Exception =
      Assertions.assertThrows(
        DataAccessException::class.java,
      ) {
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

    Assertions.assertTrue(e.message!!.contains("duplicate key value violates unique constraint \"declarative_manifest_image_version_pkey\""))
  }

  companion object {
    private const val DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE = "declarative_manifest_image_version"

    private const val MAJOR_VERSION = "major_version"
    private const val IMAGE_VERSION = "image_version"
    private const val CREATED_AT = "created_at"
    private const val UPDATED_AT = "updated_at"
  }
}
