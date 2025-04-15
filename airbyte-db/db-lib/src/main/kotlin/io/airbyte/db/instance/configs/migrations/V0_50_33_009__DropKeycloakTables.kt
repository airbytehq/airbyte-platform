/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import java.util.function.Consumer

private val log = KotlinLogging.logger {}

// Remove all keycloak tables from the public schema.
@Suppress("ktlint:standard:class-naming")
class V0_50_33_009__DropKeycloakTables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val tablesToDelete =
      listOf(
        "admin_event_entity",
        "associated_policy",
        "authentication_execution",
        "authentication_flow",
        "authenticator_config",
        "authenticator_config_entry",
        "broker_link",
        "client",
        "client_attributes",
        "client_auth_flow_bindings",
        "client_initial_access",
        "client_node_registrations",
        "client_scope",
        "client_scope_attributes",
        "client_scope_client",
        "client_scope_role_mapping",
        "client_session",
        "client_session_auth_status",
        "client_session_note",
        "client_session_prot_mapper",
        "client_session_role",
        "client_user_session_note",
        "component",
        "component_config",
        "composite_role",
        "credential",
        "databasechangelog",
        "databasechangeloglock",
        "default_client_scope",
        "event_entity",
        "fed_user_attribute",
        "fed_user_consent",
        "fed_user_consent_cl_scope",
        "fed_user_credential",
        "fed_user_group_membership",
        "fed_user_required_action",
        "fed_user_role_mapping",
        "federated_identity",
        "federated_user",
        "group_attribute",
        "group_role_mapping",
        "identity_provider",
        "identity_provider_config",
        "identity_provider_mapper",
        "idp_mapper_config",
        "keycloak_group",
        "keycloak_role",
        "migration_model",
        "offline_client_session",
        "offline_user_session",
        "policy_config",
        "protocol_mapper",
        "protocol_mapper_config",
        "realm",
        "realm_attribute",
        "realm_default_groups",
        "realm_enabled_event_types",
        "realm_events_listeners",
        "realm_localizations",
        "realm_required_credential",
        "realm_smtp_config",
        "realm_supported_locales",
        "redirect_uris",
        "required_action_config",
        "required_action_provider",
        "resource_attribute",
        "resource_policy",
        "resource_server",
        "resource_server_perm_ticket",
        "resource_server_policy",
        "resource_server_resource",
        "resource_server_scope",
        "resource_uris",
        "role_attribute",
        "scope_mapping",
        "scope_policy",
        "user_attribute",
        "user_consent",
        "user_consent_client_scope",
        "user_entity",
        "user_federation_config",
        "user_federation_mapper",
        "user_federation_mapper_config",
        "user_federation_provider",
        "user_group_membership",
        "user_required_action",
        "user_role_mapping",
        "user_session",
        "user_session_note",
        "username_login_failure",
        "web_origins",
      )

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)
    dropTables(ctx, tablesToDelete)

    log.info { "Completed migration: ${javaClass.simpleName}" }
  }

  companion object {
    @JvmStatic
    fun dropTables(
      ctx: DSLContext,
      tablesToDelete: List<String>,
    ) {
      tablesToDelete.forEach(
        Consumer { tableName: String? ->
          ctx.dropTableIfExists(DSL.table(tableName)).cascade().execute()
        },
      )
    }
  }
}
