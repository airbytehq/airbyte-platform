/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.PERMISSION_TABLE
import io.airbyte.db.instance.DatabaseConstants.USER_TABLE
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
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Migration to add User and Permission tables to the OSS Config DB. These are essentially
 * duplicated from Cloud, in preparation for introducing user-based auth to OSS.
 */
@Suppress("ktlint:standard:class-naming")
class V0_44_4_001__AddUserAndPermissionTables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    log.info { "Creating user data types, table, and indexes..." }
    createStatusEnumType(ctx)
    createAuthProviderEnumType(ctx)
    createUserTableAndIndexes(ctx)

    log.info { "Creating permission data types, table, and indexes..." }
    createPermissionTypeEnumType(ctx)
    createPermissionTableAndIndexes(ctx)

    log.info { "Migration finished!" }
  }

  /**
   * User Status enum copied from Cloud DB.
   */
  enum class Status(
    private val literal: String,
  ) : EnumType {
    INVITED("invited"),
    REGISTERED("registered"),
    DISABLED("disabled"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "status"
    }
  }

  /**
   * User AuthProvider enum copied from Cloud DB.
   */
  enum class AuthProvider(
    private val literal: String,
  ) : EnumType {
    GOOGLE_IDENTITY_PLATFORM("google_identity_platform"),
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
   * User PermissionType enum copied from Cloud DB.
   */
  enum class PermissionType(
    private val literal: String,
  ) : EnumType {
    INSTANCE_ADMIN("instance_admin"),
    WORKSPACE_OWNER("workspace_owner"),
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
    private val LOGGER: Logger = LoggerFactory.getLogger(V0_44_4_001__AddUserAndPermissionTables::class.java)

    private fun createStatusEnumType(ctx: DSLContext) {
      ctx
        .createType(Status.NAME)
        .asEnum(*Status.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun createAuthProviderEnumType(ctx: DSLContext) {
      ctx
        .createType(AuthProvider.NAME)
        .asEnum(*AuthProvider.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun createPermissionTypeEnumType(ctx: DSLContext) {
      ctx
        .createType(PermissionType.NAME)
        .asEnum(*PermissionType.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun createUserTableAndIndexes(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
      val authUserId = DSL.field("auth_user_id", SQLDataType.VARCHAR(256).nullable(false))
      val authProvider = DSL.field("auth_provider", SQLDataType.VARCHAR.asEnumDataType(AuthProvider::class.java).nullable(false))
      val defaultWorkspaceId = DSL.field("default_workspace_id", SQLDataType.UUID.nullable(true))
      val status = DSL.field("status", SQLDataType.VARCHAR.asEnumDataType(Status::class.java).nullable(true))
      val companyName = DSL.field("company_name", SQLDataType.VARCHAR(256).nullable(true))
      // this was nullable in cloud, but should be required.
      val email = DSL.field("email", SQLDataType.VARCHAR(256).nullable(false))
      val news = DSL.field("news", SQLDataType.BOOLEAN.nullable(true))
      val uiMetadata = DSL.field("ui_metadata", SQLDataType.JSONB.nullable(true))
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val updatedAt =
        DSL.field(
          "updated_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )

      ctx
        .createTableIfNotExists(USER_TABLE)
        .columns(
          id,
          name,
          authUserId,
          authProvider,
          defaultWorkspaceId,
          status,
          companyName,
          email,
          news,
          uiMetadata,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey<UUID>(defaultWorkspaceId).references("workspace", "id").onDeleteSetNull(),
        ).execute()

      ctx
        .createIndexIfNotExists("user_auth_provider_auth_user_id_idx")
        .on(USER_TABLE, "auth_provider", "auth_user_id")
        .execute()

      ctx
        .createIndexIfNotExists("user_email_idx")
        .on(USER_TABLE, "email")
        .execute()
    }

    private fun createPermissionTableAndIndexes(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val userId = DSL.field("user_id", SQLDataType.UUID.nullable(false))
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(true))
      val permissionType =
        DSL.field(
          "permission_type",
          SQLDataType.VARCHAR
            .asEnumDataType(
              PermissionType::class.java,
            ).nullable(false),
        )
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val updatedAt =
        DSL.field(
          "updated_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )

      ctx
        .createTableIfNotExists(PERMISSION_TABLE)
        .columns(
          id,
          userId,
          workspaceId,
          permissionType,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey<UUID>(userId).references(USER_TABLE, "id").onDeleteCascade(),
          DSL.foreignKey<UUID>(workspaceId).references("workspace", "id").onDeleteCascade(),
        ).execute()

      ctx
        .createIndexIfNotExists("permission_user_id_idx")
        .on(PERMISSION_TABLE, "user_id")
        .execute()

      ctx
        .createIndexIfNotExists("permission_workspace_id_idx")
        .on(PERMISSION_TABLE, "workspace_id")
        .execute()
    }
  }
}
