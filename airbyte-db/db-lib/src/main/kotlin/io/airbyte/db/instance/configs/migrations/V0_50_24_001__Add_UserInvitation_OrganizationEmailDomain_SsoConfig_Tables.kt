/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_EMAIL_DOMAIN_TABLE
import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE
import io.airbyte.db.instance.DatabaseConstants.SSO_CONFIG_TABLE
import io.airbyte.db.instance.DatabaseConstants.USER_INVITATION_TABLE
import io.airbyte.db.instance.DatabaseConstants.USER_TABLE
import io.airbyte.db.instance.DatabaseConstants.WORKSPACE_TABLE
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
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Adding new tables and corresponding indexes: UserInvitation, OrganizationEmailDomain, and
 * SsoConfig.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_24_001__Add_UserInvitation_OrganizationEmailDomain_SsoConfig_Tables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    createInvitationStatusEnumType(ctx)
    createUserInvitationTableAndIndexes(ctx)
    createOrganizationEmailDomainTableAndIndexes(ctx)
    createSsoConfigTableAndIndexes(ctx)
  }

  /**
   * new InvitationStatus enum.
   */
  enum class InvitationStatus(
    private val literal: String,
  ) : EnumType {
    PENDING("pending"),
    ACCEPTED("accepted"),
    CANCELLED("cancelled"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "invitation_status"
    }
  }

  companion object {
    private const val ORGANIZATION_ID = "organization_id"

    private fun createInvitationStatusEnumType(ctx: DSLContext) {
      ctx
        .createType(InvitationStatus.NAME)
        .asEnum(*InvitationStatus.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun createUserInvitationTableAndIndexes(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val inviteCode = DSL.field("invite_code", SQLDataType.VARCHAR(256).nullable(false))
      val inviterUserId = DSL.field("inviter_user_id", SQLDataType.UUID.nullable(false))
      val invitedEmail = DSL.field("invited_email", SQLDataType.VARCHAR(256).nullable(false))
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(true))
      val organizationId = DSL.field(ORGANIZATION_ID, SQLDataType.UUID.nullable(true))
      val permissionType =
        DSL.field(
          "permission_type",
          SQLDataType.VARCHAR.asEnumDataType(V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType::class.java).nullable(false),
        )
      val status = DSL.field("status", SQLDataType.VARCHAR.asEnumDataType(InvitationStatus::class.java).nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(USER_INVITATION_TABLE)
        .columns(
          id,
          inviteCode,
          inviterUserId,
          invitedEmail,
          workspaceId,
          organizationId,
          permissionType,
          status,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id), // an invite code needs to be able to uniquely identify an invitation.
          DSL.unique(inviteCode), // don't remove an invitation if the inviter is deleted.
          DSL
            .foreignKey<UUID>(inviterUserId)
            .references(USER_TABLE, "id")
            .onDeleteNoAction(), // if a workspace is deleted, remove any invitation to that workspace.
          DSL
            .foreignKey<UUID>(workspaceId)
            .references(WORKSPACE_TABLE, "id")
            .onDeleteCascade(), // if an organization is deleted, remove any invitation to that organization.
          DSL.foreignKey<UUID>(organizationId).references(ORGANIZATION_TABLE, "id").onDeleteCascade(),
        ).execute()

      // create an index on invite_code, for efficient lookups of a particular invitation by invite code.
      ctx
        .createIndexIfNotExists("user_invitation_invite_code_idx")
        .on(USER_INVITATION_TABLE, "invite_code")
        .execute()

      // create an index on invited email, for efficient lookups of all invitations sent to a particular
      // email address.
      ctx
        .createIndexIfNotExists("user_invitation_invited_email_idx")
        .on(USER_INVITATION_TABLE, "invited_email")
        .execute()

      // create an index on workspace id, for efficient lookups of all invitations to a particular
      // workspace.
      ctx
        .createIndexIfNotExists("user_invitation_workspace_id_idx")
        .on(USER_INVITATION_TABLE, "workspace_id")
        .execute()

      // create an index on organization id, for efficient lookups of all invitations to a particular
      // organization.
      ctx
        .createIndexIfNotExists("user_invitation_organization_id_idx")
        .on(USER_INVITATION_TABLE, ORGANIZATION_ID)
        .execute()
    }

    private fun createOrganizationEmailDomainTableAndIndexes(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val organizationId = DSL.field(ORGANIZATION_ID, SQLDataType.UUID.nullable(false))
      val emailDomain = DSL.field("email_domain", SQLDataType.VARCHAR(256).nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(ORGANIZATION_EMAIL_DOMAIN_TABLE)
        .columns(
          id,
          organizationId,
          emailDomain,
          createdAt,
        ).constraints(
          DSL.primaryKey(id), // an email domain can only be associated with one organization.
          DSL.unique(emailDomain), // if an organization is deleted, remove any email domains associated with that organization.
          DSL.foreignKey<UUID>(organizationId).references(ORGANIZATION_TABLE, "id").onDeleteCascade(),
        ).execute()

      // create an index on domain, for efficient lookups of a particular email domain.
      ctx
        .createIndexIfNotExists("organization_email_domain_email_domain_idx")
        .on("organization_email_domain", "email_domain")
        .execute()

      // create an index on organization id, for efficient lookups of all email domains associated with a
      // particular organization.
      ctx
        .createIndexIfNotExists("organization_email_domain_organization_id_idx")
        .on("organization_email_domain", ORGANIZATION_ID)
        .execute()
    }

    private fun createSsoConfigTableAndIndexes(ctx: DSLContext) {
      // create table with id, organization_id, and keycloak_realm fields
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val organizationId = DSL.field(ORGANIZATION_ID, SQLDataType.UUID.nullable(false))
      val keycloakRealm = DSL.field("keycloak_realm", SQLDataType.VARCHAR(256).nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(SSO_CONFIG_TABLE)
        .columns(
          id,
          organizationId,
          keycloakRealm,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id), // an organization can only have one SSO config.
          DSL.unique(organizationId), // a keycloak realm can only be associated with one organization.
          DSL.unique(keycloakRealm), // if an organization is deleted, remove any SSO config associated with that organization.
          DSL.foreignKey<UUID>(organizationId).references(ORGANIZATION_TABLE, "id").onDeleteCascade(),
        ).execute()

      // create an index on organization id, for efficient lookups of a particular organization's SSO
      // config.
      ctx
        .createIndexIfNotExists("sso_config_organization_id_idx")
        .on(SSO_CONFIG_TABLE, ORGANIZATION_ID)
        .execute()

      // create an index on keycloak realm, for efficient lookups of a particular SSO config by keycloak
      // realm.
      ctx
        .createIndexIfNotExists("sso_config_keycloak_realm_idx")
        .on(SSO_CONFIG_TABLE, "keycloak_realm")
        .execute()
    }
  }
}
