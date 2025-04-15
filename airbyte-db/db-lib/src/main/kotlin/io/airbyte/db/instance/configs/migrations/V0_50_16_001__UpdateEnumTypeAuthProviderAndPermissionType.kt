/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

/**
 * Update enum type: AuthProvider (in User table) and PermissionType (in Permission table). Note: At
 * the time updating these enums, User table and Permission table are still empty in OSS, so there
 * is no real data migration needed.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    updateAuthProviderEnumType(ctx)
    updatePermissionTypeEnumType(ctx)
    log.info { "Migration finished!" }
  }

  /**
   * User AuthProvider enums.
   */
  enum class AuthProvider(
    private val literal: String,
  ) : EnumType {
    AIRBYTE("airbyte"),
    GOOGLE_IDENTITY_PLATFORM("google_identity_platform"),
    KEYCLOAK("keycloak"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "auth_provider"
    }
  }

  /**
   * User Roles as PermissionType enums.
   */
  enum class PermissionType(
    private val literal: String,
  ) : EnumType {
    INSTANCE_ADMIN("instance_admin"),
    ORGANIZATION_ADMIN("organization_admin"),
    ORGANIZATION_EDITOR("organization_editor"),
    ORGANIZATION_READER("organization_reader"),
    WORKSPACE_ADMIN("workspace_admin"),
    WORKSPACE_EDITOR("workspace_editor"),
    WORKSPACE_READER("workspace_reader"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "permission_type"
    }
  }

  companion object {
    private const val USER_TABLE = "user"
    private const val PERMISSION_TABLE = "permission"
    private const val AUTH_PROVIDER_COLUMN_NAME = "auth_provider"
    private const val PERMISSION_TYPE_COLUMN_NAME = "permission_type"

    private val OLD_AUTH_PROVIDER_COLUMN =
      DSL.field(
        AUTH_PROVIDER_COLUMN_NAME,
        SQLDataType.VARCHAR.asEnumDataType(V0_44_4_001__AddUserAndPermissionTables.AuthProvider::class.java).nullable(false),
      )
    private val NEW_AUTH_PROVIDER_COLUMN =
      DSL.field(
        AUTH_PROVIDER_COLUMN_NAME,
        SQLDataType.VARCHAR.asEnumDataType(AuthProvider::class.java).nullable(false),
      )

    private val OLD_PERMISSION_TYPE_COLUMN =
      DSL.field(
        PERMISSION_TYPE_COLUMN_NAME,
        SQLDataType.VARCHAR.asEnumDataType(V0_44_4_001__AddUserAndPermissionTables.PermissionType::class.java).nullable(false),
      )
    private val NEW_PERMISSION_TYPE_COLUMN =
      DSL.field(
        PERMISSION_TYPE_COLUMN_NAME,
        SQLDataType.VARCHAR.asEnumDataType(PermissionType::class.java).nullable(false),
      )

    fun updateAuthProviderEnumType(ctx: DSLContext) {
      ctx.alterTable(USER_TABLE).dropColumn(OLD_AUTH_PROVIDER_COLUMN).execute()
      ctx.dropTypeIfExists(AuthProvider.NAME).execute()
      ctx
        .createType(AuthProvider.NAME)
        .asEnum(*AuthProvider.entries.map { it.literal }.toTypedArray())
        .execute()
      ctx.alterTable(USER_TABLE).addColumn(NEW_AUTH_PROVIDER_COLUMN).execute()
      ctx
        .createIndexIfNotExists("user_auth_provider_auth_user_id_idx")
        .on(USER_TABLE, "auth_provider", "auth_user_id")
        .execute()
    }

    fun updatePermissionTypeEnumType(ctx: DSLContext) {
      ctx.alterTable(PERMISSION_TABLE).dropColumn(OLD_PERMISSION_TYPE_COLUMN).execute()
      ctx.dropTypeIfExists(PermissionType.NAME).execute()
      ctx
        .createType(PermissionType.NAME)
        .asEnum(*PermissionType.entries.map { it.literal }.toTypedArray())
        .execute()
      ctx.alterTable(PERMISSION_TABLE).addColumn(NEW_PERMISSION_TYPE_COLUMN).execute()
    }
  }
}
