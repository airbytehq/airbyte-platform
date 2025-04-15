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
import kotlin.jvm.optionals.getOrNull

private val log = KotlinLogging.logger {}

/**
 * This migration ensures that every instance of Airbyte has a default organization and user. For
 * instances that have already been set up with an email address persisted on the default workspace,
 * this migration will copy that email address to the default organization and user. Otherwise,
 * email will be blank.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_19_001__CreateDefaultOrganizationAndUser : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createDefaultUserAndOrganization(ctx)
  }

  companion object {
    private const val WORKSPACE_TABLE = "workspace"

    // The user table is quoted to avoid conflict with the reserved user keyword in Postgres.
    private const val USER_TABLE = "\"user\""
    private const val ORGANIZATION_TABLE = "organization"
    private const val PERMISSION_TABLE = "permission"

    // Default values
    private val DEFAULT_AUTH_PROVIDER =
      V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider.AIRBYTE
    private const val DEFAULT_USER_NAME = "Default User"
    private val DEFAULT_USER_STATUS = V0_44_4_001__AddUserAndPermissionTables.Status.REGISTERED
    private const val DEFAULT_ORGANIZATION_NAME = "Default Organization"

    // The all-zero UUID is used to reliably identify the default organization and user.
    private val DEFAULT_USER_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
    private val DEFAULT_ORGANIZATION_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")

    // The default email address is blank, since it is a non-nullable column.
    private const val DEFAULT_EMAIL = ""

    // Shared fields
    private val ID_COLUMN = DSL.field("id", SQLDataType.UUID)
    private val EMAIL_COLUMN = DSL.field("email", SQLDataType.VARCHAR(256))
    private val NAME_COLUMN = DSL.field("name", SQLDataType.VARCHAR(256))
    private val USER_ID_COLUMN = DSL.field("user_id", SQLDataType.UUID)

    // Workspace specific fields
    private val INITIAL_SETUP_COMPLETE_COLUMN = DSL.field("initial_setup_complete", SQLDataType.BOOLEAN)
    private val TOMBSTONE_COLUMN = DSL.field("tombstone", SQLDataType.BOOLEAN)
    private val ORGANIZATION_ID_COLUMN = DSL.field("organization_id", SQLDataType.UUID)

    // User specific fields
    private val AUTH_USER_ID_COLUMN = DSL.field("auth_user_id", SQLDataType.VARCHAR(256))
    private val DEFAULT_WORKSPACE_ID_COLUMN = DSL.field("default_workspace_id", SQLDataType.UUID)
    private val STATUS_COLUMN =
      DSL.field(
        "status",
        SQLDataType.VARCHAR.asEnumDataType(V0_44_4_001__AddUserAndPermissionTables.Status::class.java),
      )
    private val AUTH_PROVIDER_COLUMN =
      DSL.field(
        "auth_provider",
        SQLDataType.VARCHAR.asEnumDataType(V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider::class.java),
      )

    // Permission specific fields
    private val PERMISSION_TYPE_COLUMN =
      DSL.field(
        "permission_type",
        SQLDataType.VARCHAR.asEnumDataType(V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType::class.java),
      )

    @JvmStatic
    fun createDefaultUserAndOrganization(ctx: DSLContext) {
      // return early if a default user or default organization already exist.
      // this shouldn't happen in practice, but if this migration somehow gets run
      // multiple times or an instance is for some reason already using the
      // all-zero UUID, we don't want to overwrite any existing records.
      if (ctx.fetchExists(
          DSL
            .select()
            .from(DSL.table(USER_TABLE))
            .where(ID_COLUMN.eq(DEFAULT_USER_ID)),
        )
      ) {
        log.info { "Default user already exists. Skipping this migration." }
        return
      }

      if (ctx.fetchExists(
          DSL
            .select()
            .from(DSL.table(ORGANIZATION_TABLE))
            .where(ID_COLUMN.eq(DEFAULT_ORGANIZATION_ID)),
        )
      ) {
        log.info { "Default organization already exists. Skipping this migration." }
        return
      }

      val workspaceId = getDefaultWorkspaceId(ctx)
      val email = workspaceId?.let { getWorkspaceEmail(ctx, it) } ?: DEFAULT_EMAIL

      // insert the default User record
      ctx
        .insertInto(DSL.table(USER_TABLE))
        .columns(
          ID_COLUMN,
          EMAIL_COLUMN,
          NAME_COLUMN,
          AUTH_USER_ID_COLUMN,
          DEFAULT_WORKSPACE_ID_COLUMN,
          STATUS_COLUMN,
          AUTH_PROVIDER_COLUMN,
        ).values(
          DEFAULT_USER_ID,
          email,
          DEFAULT_USER_NAME,
          DEFAULT_USER_ID.toString(),
          workspaceId,
          DEFAULT_USER_STATUS,
          DEFAULT_AUTH_PROVIDER,
        ).execute()

      ctx
        .insertInto(DSL.table(ORGANIZATION_TABLE))
        .columns(ID_COLUMN, EMAIL_COLUMN, NAME_COLUMN, USER_ID_COLUMN)
        .values(DEFAULT_ORGANIZATION_ID, email, DEFAULT_ORGANIZATION_NAME, DEFAULT_USER_ID)
        .execute()

      // update the default workspace to point to the default organization
      workspaceId?.let {
        log.info {
          "Updating default workspace with ID $it to belong to default organization with ID $DEFAULT_ORGANIZATION_ID"
        }
        ctx
          .update(DSL.table(WORKSPACE_TABLE))
          .set(ORGANIZATION_ID_COLUMN, DEFAULT_ORGANIZATION_ID)
          .where(ID_COLUMN.eq(it))
          .execute()
      } ?: run {
        log.info { "No default workspace found. Skipping update of default workspace to point to default organization." }
      }

      // grant the default user admin permissions on the default organization
      log.info {
        "Granting ORGANIZATION_ADMIN permission to default user with ID $DEFAULT_USER_ID on default organization with ID $DEFAULT_ORGANIZATION_ID"
      }
      ctx
        .insertInto(DSL.table(PERMISSION_TABLE))
        .columns(ID_COLUMN, USER_ID_COLUMN, ORGANIZATION_ID_COLUMN, PERMISSION_TYPE_COLUMN)
        .values(
          UUID.randomUUID(),
          DEFAULT_USER_ID,
          DEFAULT_ORGANIZATION_ID,
          V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.ORGANIZATION_ADMIN,
        ).execute()
    }

    // Return the first non-tombstoned workspace in the instance with `initialSetupComplete: true`, if
    // it exists. Otherwise, just return the first workspace in the instance regardless of its setup
    // status, if it exists.
    private fun getDefaultWorkspaceId(ctx: DSLContext): UUID? {
      val setupWorkspaceId =
        ctx
          .select(ID_COLUMN)
          .from(WORKSPACE_TABLE)
          .where(INITIAL_SETUP_COMPLETE_COLUMN.eq(true))
          .and(TOMBSTONE_COLUMN.eq(false))
          .limit(1)
          .fetchOptional(ID_COLUMN)
          .getOrNull()

      // return the optional ID if it is present. Otherwise, return the first non-tombstoned workspace ID in the database.
      return setupWorkspaceId ?: run {
        ctx
          .select(ID_COLUMN)
          .from(WORKSPACE_TABLE)
          .where(TOMBSTONE_COLUMN.eq(false))
          .limit(1)
          .fetchOptional(ID_COLUMN)
          .getOrNull()
      }
    }

    // Find the email address of the default workspace, if it exists.
    private fun getWorkspaceEmail(
      ctx: DSLContext,
      workspaceId: UUID?,
    ): String? =
      ctx
        .select(EMAIL_COLUMN)
        .from(WORKSPACE_TABLE)
        .where(ID_COLUMN.eq(workspaceId))
        .fetchOptional(EMAIL_COLUMN)
        .getOrNull()
  }
}
