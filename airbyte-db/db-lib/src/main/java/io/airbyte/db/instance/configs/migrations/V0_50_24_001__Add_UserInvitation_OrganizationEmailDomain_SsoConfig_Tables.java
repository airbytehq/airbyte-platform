/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_EMAIL_DOMAIN_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.SSO_CONFIG_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.USER_INVITATION_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.USER_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.WORKSPACE_TABLE;
import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.unique;

import io.airbyte.db.instance.configs.migrations.V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.Catalog;
import org.jooq.DSLContext;
import org.jooq.EnumType;
import org.jooq.Field;
import org.jooq.Schema;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adding new tables and corresponding indexes: UserInvitation, OrganizationEmailDomain, and
 * SsoConfig.
 */
public class V0_50_24_001__Add_UserInvitation_OrganizationEmailDomain_SsoConfig_Tables extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_24_001__Add_UserInvitation_OrganizationEmailDomain_SsoConfig_Tables.class);

  private static final String ORGANIZATION_ID = "organization_id";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    createInvitationStatusEnumType(ctx);
    createUserInvitationTableAndIndexes(ctx);
    createOrganizationEmailDomainTableAndIndexes(ctx);
    createSsoConfigTableAndIndexes(ctx);
  }

  private static void createInvitationStatusEnumType(final DSLContext ctx) {
    ctx.createType(InvitationStatus.NAME)
        .asEnum(Arrays.stream(InvitationStatus.values()).map(InvitationStatus::getLiteral).toArray(String[]::new))
        .execute();
  }

  private static void createUserInvitationTableAndIndexes(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<String> inviteCode = DSL.field("invite_code", SQLDataType.VARCHAR(256).nullable(false));
    final Field<UUID> inviterUserId = DSL.field("inviter_user_id", SQLDataType.UUID.nullable(false));
    final Field<String> invitedEmail = DSL.field("invited_email", SQLDataType.VARCHAR(256).nullable(false));
    final Field<UUID> workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(true));
    final Field<UUID> organizationId = DSL.field(ORGANIZATION_ID, SQLDataType.UUID.nullable(true));
    final Field<PermissionType> permissionType =
        DSL.field("permission_type", SQLDataType.VARCHAR.asEnumDataType(PermissionType.class).nullable(false));
    final Field<InvitationStatus> status =
        DSL.field("status", SQLDataType.VARCHAR.asEnumDataType(InvitationStatus.class).nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists(USER_INVITATION_TABLE)
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
            updatedAt)
        .constraints(
            primaryKey(id),
            // an invite code needs to be able to uniquely identify an invitation.
            unique(inviteCode),
            // don't remove an invitation if the inviter is deleted.
            foreignKey(inviterUserId).references(USER_TABLE, "id").onDeleteNoAction(),
            // if a workspace is deleted, remove any invitation to that workspace.
            foreignKey(workspaceId).references(WORKSPACE_TABLE, "id").onDeleteCascade(),
            // if an organization is deleted, remove any invitation to that organization.
            foreignKey(organizationId).references(ORGANIZATION_TABLE, "id").onDeleteCascade())
        .execute();

    // create an index on invite_code, for efficient lookups of a particular invitation by invite code.
    ctx.createIndexIfNotExists("user_invitation_invite_code_idx")
        .on(USER_INVITATION_TABLE, "invite_code")
        .execute();

    // create an index on invited email, for efficient lookups of all invitations sent to a particular
    // email address.
    ctx.createIndexIfNotExists("user_invitation_invited_email_idx")
        .on(USER_INVITATION_TABLE, "invited_email")
        .execute();

    // create an index on workspace id, for efficient lookups of all invitations to a particular
    // workspace.
    ctx.createIndexIfNotExists("user_invitation_workspace_id_idx")
        .on(USER_INVITATION_TABLE, "workspace_id")
        .execute();

    // create an index on organization id, for efficient lookups of all invitations to a particular
    // organization.
    ctx.createIndexIfNotExists("user_invitation_organization_id_idx")
        .on(USER_INVITATION_TABLE, ORGANIZATION_ID)
        .execute();
  }

  private static void createOrganizationEmailDomainTableAndIndexes(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<UUID> organizationId = DSL.field(ORGANIZATION_ID, SQLDataType.UUID.nullable(false));
    final Field<String> emailDomain = DSL.field("email_domain", SQLDataType.VARCHAR(256).nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists(ORGANIZATION_EMAIL_DOMAIN_TABLE)
        .columns(
            id,
            organizationId,
            emailDomain,
            createdAt)
        .constraints(
            primaryKey(id),
            // an email domain can only be associated with one organization.
            unique(emailDomain),
            // if an organization is deleted, remove any email domains associated with that organization.
            foreignKey(organizationId).references(ORGANIZATION_TABLE, "id").onDeleteCascade())
        .execute();

    // create an index on domain, for efficient lookups of a particular email domain.
    ctx.createIndexIfNotExists("organization_email_domain_email_domain_idx")
        .on("organization_email_domain", "email_domain")
        .execute();

    // create an index on organization id, for efficient lookups of all email domains associated with a
    // particular organization.
    ctx.createIndexIfNotExists("organization_email_domain_organization_id_idx")
        .on("organization_email_domain", ORGANIZATION_ID)
        .execute();
  }

  private static void createSsoConfigTableAndIndexes(final DSLContext ctx) {
    // create table with id, organization_id, and keycloak_realm fields
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<UUID> organizationId = DSL.field(ORGANIZATION_ID, SQLDataType.UUID.nullable(false));
    final Field<String> keycloakRealm = DSL.field("keycloak_realm", SQLDataType.VARCHAR(256).nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists(SSO_CONFIG_TABLE)
        .columns(
            id,
            organizationId,
            keycloakRealm,
            createdAt,
            updatedAt)
        .constraints(
            primaryKey(id),
            // an organization can only have one SSO config.
            unique(organizationId),
            // a keycloak realm can only be associated with one organization.
            unique(keycloakRealm),
            // if an organization is deleted, remove any SSO config associated with that organization.
            foreignKey(organizationId).references(ORGANIZATION_TABLE, "id").onDeleteCascade())
        .execute();

    // create an index on organization id, for efficient lookups of a particular organization's SSO
    // config.
    ctx.createIndexIfNotExists("sso_config_organization_id_idx")
        .on(SSO_CONFIG_TABLE, ORGANIZATION_ID)
        .execute();

    // create an index on keycloak realm, for efficient lookups of a particular SSO config by keycloak
    // realm.
    ctx.createIndexIfNotExists("sso_config_keycloak_realm_idx")
        .on(SSO_CONFIG_TABLE, "keycloak_realm")
        .execute();
  }

  /**
   * new InvitationStatus enum.
   */
  public enum InvitationStatus implements EnumType {

    PENDING("pending"),
    ACCEPTED("accepted"),
    CANCELLED("cancelled");

    private final String literal;
    public static final String NAME = "invitation_status";

    InvitationStatus(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"));
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
