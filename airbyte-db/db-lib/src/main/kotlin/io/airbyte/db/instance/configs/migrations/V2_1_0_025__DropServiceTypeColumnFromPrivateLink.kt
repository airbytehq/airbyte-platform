/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.PRIVATE_LINK_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Drops the redundant `service_type` column. The discriminator now lives inside
 * `service_config` JSONB as the `type` key, matching the OpenAPI 3.0 discriminator
 * pattern used elsewhere in this codebase (see `RowFilteringOperation.type` and
 * `EncryptionMapperConfiguration.algorithm`).
 *
 * Two backfill passes before the column drop:
 * 1. Rows where Jackson previously wrote `serviceType` (uppercase enum name): rename
 *    that key to `type` and lowercase its value.
 * 2. Rows backfilled by V2_1_0_024 (no internal discriminator yet): add `type` using
 *    the column value (already lowercase from the column's enum literal).
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_025__DropServiceTypeColumnFromPrivateLink : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx: DSLContext = DSL.using(context.connection)

    renameLegacyServiceTypeKey(ctx)
    backfillTypeFromColumn(ctx)
    backfillVersionIfMissing(ctx)
    dropServiceTypeColumn(ctx)
    dropServiceTypeEnumType(ctx)
  }

  companion object {
    /**
     * Rows written by Kotlin after V024 have `serviceType` inside the JSONB
     * (Jackson serialized the sealed-class override val as the uppercase enum name).
     * Rename to `type` and lowercase the value to match the column convention.
     */
    private fun renameLegacyServiceTypeKey(ctx: DSLContext) {
      ctx
        .execute(
          """
          UPDATE $PRIVATE_LINK_TABLE
          SET service_config =
            (service_config - 'serviceType')
            || jsonb_build_object('type', LOWER(service_config->>'serviceType'))
          WHERE service_config ?? 'serviceType'
          """.trimIndent(),
        )
    }

    /**
     * Rows backfilled by V024 have no internal discriminator. Copy the column value
     * into JSONB as `type` so the column can be dropped safely.
     */
    private fun backfillTypeFromColumn(ctx: DSLContext) {
      ctx
        .execute(
          """
          UPDATE $PRIVATE_LINK_TABLE
          SET service_config =
            service_config || jsonb_build_object('type', service_type::text)
          WHERE NOT (service_config ?? 'type')
          """.trimIndent(),
        )
    }

    /**
     * Every row should carry a `version` stamp in `service_config` so future readers can tell
     * shape generations apart (matches the intent of V024's backfill). Pre-this-migration
     * Kotlin writes omitted the field; backfill it here.
     */
    private fun backfillVersionIfMissing(ctx: DSLContext) {
      ctx
        .execute(
          """
          UPDATE $PRIVATE_LINK_TABLE
          SET service_config = service_config || jsonb_build_object('version', 1)
          WHERE NOT (service_config ?? 'version')
          """.trimIndent(),
        )
    }

    private fun dropServiceTypeColumn(ctx: DSLContext) {
      ctx
        .alterTable(PRIVATE_LINK_TABLE)
        .dropColumnIfExists("service_type")
        .execute()
    }

    private fun dropServiceTypeEnumType(ctx: DSLContext) {
      ctx.execute("DROP TYPE IF EXISTS private_link_service_type")
    }
  }
}
