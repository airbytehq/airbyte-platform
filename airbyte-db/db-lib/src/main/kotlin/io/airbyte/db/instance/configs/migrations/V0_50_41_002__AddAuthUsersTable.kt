/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.commons.enums.Enums
import io.airbyte.db.instance.DatabaseConstants.AUTH_USER_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.Record5
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.time.OffsetDateTime
import java.util.UUID
import java.util.function.Consumer

private val log = KotlinLogging.logger {}

/**
 * Migration to add the AuthUser table to the OSS Config DB. This table will replace the columns
 * auth_user_id and auth_provider in the user table to allow a 1:n relationship between user and
 * auth_user
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_41_002__AddAuthUsersTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    createAuthUsersTable(ctx)
    populateAuthUserTable(ctx)
  }

  companion object {
    private const val USER_TABLE = """"user""""

    private val idField = DSL.field("id", SQLDataType.UUID.nullable(false))
    private val userIdField = DSL.field("user_id", SQLDataType.UUID.nullable(false))
    private val authUserIdField = DSL.field("auth_user_id", SQLDataType.VARCHAR(256).nullable(false))
    private val authProviderField =
      DSL.field(
        "auth_provider",
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider::class.java,
          ).nullable(false),
      )
    private val createdAtField =
      DSL.field(
        "created_at",
        SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(
          DSL.currentOffsetDateTime(),
        ),
      )
    private val updatedAtField =
      DSL.field(
        "updated_at",
        SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(
          DSL.currentOffsetDateTime(),
        ),
      )

    @JvmStatic
    fun createAuthUsersTable(ctx: DSLContext) {
      ctx
        .createTable(AUTH_USER_TABLE)
        .columns(
          idField,
          userIdField,
          authUserIdField,
          authProviderField,
          createdAtField,
          updatedAtField,
        ).constraints(
          DSL.primaryKey(idField),
          DSL.foreignKey<UUID>(userIdField).references("user", "id").onDeleteCascade(),
          DSL.unique(authUserIdField, authProviderField),
        ).execute()
    }

    @JvmStatic
    fun populateAuthUserTable(ctx: DSLContext) {
      val userRecords =
        ctx
          .select(
            DSL.field("id"),
            DSL.field("auth_user_id"),
            DSL.field("auth_provider"),
            DSL.field("created_at"),
            DSL.field("updated_at"),
          ).from(DSL.table(USER_TABLE))
          .fetch()

      userRecords.forEach(
        Consumer<Record5<Any, Any, Any, Any, Any>> { userRecord: Record5<Any, Any, Any, Any, Any> ->
          val now = OffsetDateTime.now()
          ctx
            .insertInto(DSL.table(AUTH_USER_TABLE))
            .set(idField, UUID.randomUUID())
            .set(
              userIdField,
              userRecord.get(
                DSL.field(
                  "id",
                  UUID::class.java,
                ),
              ),
            ).set(
              authUserIdField,
              userRecord.get(
                DSL.field(
                  "auth_user_id",
                  String::class.java,
                ),
              ),
            ).set(
              authProviderField,
              Enums
                .toEnum(
                  userRecord.get(
                    DSL.field(
                      "auth_provider",
                      String::class.java,
                    ),
                  ),
                  V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider::class.java,
                ).orElseThrow(),
            ).set(createdAtField, now)
            .set(updatedAtField, now)
            .execute()
        },
      )
    }
  }
}
