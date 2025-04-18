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
class V0_57_4_004__AddDeclarativeManifestImageVersionTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createDeclarativeManifestImageVersionTable(ctx)
  }

  companion object {
    private const val DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE = "declarative_manifest_image_version"
    private val majorVersion = DSL.field("major_version", SQLDataType.INTEGER.nullable(false))
    private val imageVersion = DSL.field("image_version", SQLDataType.VARCHAR(256).nullable(false))

    private val createdAtField =
      DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
    private val updatedAtField =
      DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

    @JvmStatic
    fun createDeclarativeManifestImageVersionTable(ctx: DSLContext) {
      ctx
        .createTable(DECLARATIVE_MANIFEST_IMAGE_VERSION_TABLE)
        .columns(majorVersion, imageVersion, createdAtField, updatedAtField)
        .constraints(DSL.primaryKey(majorVersion))
        .execute()
    }
  }
}
