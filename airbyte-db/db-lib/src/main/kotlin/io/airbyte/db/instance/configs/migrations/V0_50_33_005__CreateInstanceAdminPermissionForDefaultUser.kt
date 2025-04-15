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
import java.util.UUID

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_33_005__CreateInstanceAdminPermissionForDefaultUser : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createInstanceAdminPermissionForDefaultUser(ctx)
  }

  companion object {
    private const val PERMISSION_TABLE = "permission"
    private const val USER_TABLE = """"user""""

    // The all-zero UUID is used to reliably identify the default user.
    private val DEFAULT_USER_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
    private val ID_COLUMN = DSL.field("id", SQLDataType.UUID)
    private val USER_ID_COLUMN = DSL.field("user_id", SQLDataType.UUID)
    private val PERMISSION_TYPE_COLUMN =
      DSL.field(
        "permission_type",
        SQLDataType.VARCHAR.asEnumDataType(
          V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType::class.java,
        ),
      )

    @JvmStatic
    fun createInstanceAdminPermissionForDefaultUser(ctx: DSLContext) {
      // return early if the default user is not present in the database. This
      // shouldn't happen in practice, but if somebody manually removed the default
      // user prior to this migration, we want this to be a no-op.
      if (!ctx.fetchExists(
          DSL
            .select()
            .from(DSL.table(USER_TABLE))
            .where(ID_COLUMN.eq(DEFAULT_USER_ID)),
        )
      ) {
        log.warn { "Default user does not exist. Skipping this migration." }
        return
      }

      // return early if the default user already has an instance_admin permission.
      // This shouldn't happen in practice, but if somebody manually inserted a
      // permission record prior to this migration, we want this to be a no-op.
      if (ctx.fetchExists(
          DSL
            .select()
            .from(DSL.table(PERMISSION_TABLE))
            .where(USER_ID_COLUMN.eq(DEFAULT_USER_ID))
            .and(PERMISSION_TYPE_COLUMN.eq(V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.INSTANCE_ADMIN)),
        )
      ) {
        log.warn { "Default user already has instance_admin permission. Skipping this migration." }
        return
      }

      log.info { "Inserting instance_admin permission record for default user." }
      ctx
        .insertInto(
          DSL.table(PERMISSION_TABLE),
          ID_COLUMN,
          USER_ID_COLUMN,
          PERMISSION_TYPE_COLUMN,
        ).values(
          UUID.randomUUID(),
          DEFAULT_USER_ID,
          V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.INSTANCE_ADMIN,
        ).execute()
    }
  }
}
