/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.config.ScopeType
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

@Suppress("ktlint:standard:class-naming")
class V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    runMigration(ctx)
  }

  /**
   * User Roles as PermissionType enums.
   */
  internal enum class PermissionType(
    private val literal: String,
  ) : EnumType {
    INSTANCE_ADMIN("instance_admin"),
    ORGANIZATION_ADMIN("organization_admin"),
    ORGANIZATION_EDITOR("organization_editor"),
    ORGANIZATION_RUNNER("organization_runner"),
    ORGANIZATION_READER("organization_reader"),
    WORKSPACE_ADMIN("workspace_admin"),
    WORKSPACE_EDITOR("workspace_editor"),
    WORKSPACE_RUNNER("workspace_runner"),
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

  @Suppress("ktlint:standard:enum-entry-name-case")
  internal enum class ScopeTypeEnum(
    private val literal: String,
  ) : EnumType {
    workspace(ScopeType.WORKSPACE.value()),
    organization(ScopeType.ORGANIZATION.value()),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "scope_type"

    override fun getLiteral(): String = literal
  }

  companion object {
    // user invitation table
    private val USER_INVITATION_TABLE = DSL.table("user_invitation")
    private const val CONSTRAINT_NAME = "user_invitation_scope_permission_mismatch"
    private val PERMISSION_TYPE =
      DSL.field(
        "permission_type",
        SQLDataType.VARCHAR.asEnumDataType(
          PermissionType::class.java,
        ),
      )
    private val SCOPE_TYPE =
      DSL.field(
        "scope_type",
        SQLDataType.VARCHAR.asEnumDataType(
          ScopeTypeEnum::class.java,
        ),
      )
    private val ORGANIZATION_PERMISSION_TYPES: List<PermissionType?> =
      java.util.List.of(
        PermissionType.ORGANIZATION_ADMIN,
        PermissionType.ORGANIZATION_EDITOR,
        PermissionType.ORGANIZATION_RUNNER,
        PermissionType.ORGANIZATION_READER,
      )
    private val WORKSPACE_PERMISSION_TYPES: List<PermissionType?> =
      java.util.List.of(
        PermissionType.WORKSPACE_ADMIN,
        PermissionType.WORKSPACE_EDITOR,
        PermissionType.WORKSPACE_READER,
        PermissionType.WORKSPACE_RUNNER,
      )

    @JvmStatic
    fun runMigration(ctx: DSLContext) {
      deleteInvalidRows(ctx)
      dropConstraintIfExists(ctx)
      addConstraint(ctx)
    }

    private fun deleteInvalidRows(ctx: DSLContext) {
      ctx
        .deleteFrom(USER_INVITATION_TABLE)
        .where(
          SCOPE_TYPE
            .eq(ScopeTypeEnum.workspace)
            .and(PERMISSION_TYPE.notIn(WORKSPACE_PERMISSION_TYPES))
            .or(
              SCOPE_TYPE
                .eq(ScopeTypeEnum.organization)
                .and(PERMISSION_TYPE.notIn(ORGANIZATION_PERMISSION_TYPES)),
            ),
        ).execute()
    }

    @JvmStatic
    fun dropConstraintIfExists(ctx: DSLContext) {
      ctx.alterTable(USER_INVITATION_TABLE).dropConstraintIfExists(CONSTRAINT_NAME).execute()
    }

    private fun addConstraint(ctx: DSLContext) {
      ctx
        .alterTable(USER_INVITATION_TABLE)
        .add(
          DSL.constraint(CONSTRAINT_NAME).check(
            SCOPE_TYPE
              .eq(ScopeTypeEnum.workspace)
              .and(PERMISSION_TYPE.`in`(WORKSPACE_PERMISSION_TYPES))
              .or(
                SCOPE_TYPE
                  .eq(ScopeTypeEnum.organization)
                  .and(PERMISSION_TYPE.`in`(ORGANIZATION_PERMISSION_TYPES)),
              ),
          ),
        ).execute()
    }
  }
}
