/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import java.util.Arrays;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.exception.IOException;
import org.jooq.impl.DSL;
import org.junit.Test;

public class V0_50_33_009__DropKeycloakTablesTest extends AbstractConfigsDatabaseTest {

  @Test
  void testDropTables() throws IOException {
    final DSLContext context = getDslContext();

    List<String> tablesToDelete = Arrays.asList(
        "admin_event_entity", "associated_policy", "authentication_execution", "authentication_flow", "authenticator_config",
        "authenticator_config_entry", "broker_link", "client", "client_attributes", "client_auth_flow_bindings", "client_initial_access",
        "client_node_registrations", "client_scope", "client_scope_attributes", "client_scope_client", "client_scope_role_mapping", "client_session",
        "client_session_auth_status", "client_session_note", "client_session_prot_mapper", "client_session_role", "client_user_session_note",
        "component", "component_config", "composite_role", "credential", "databasechangelog", "databasechangeloglock", "default_client_scope",
        "event_entity", "fed_user_attribute", "fed_user_consent", "fed_user_consent_cl_scope", "fed_user_credential", "fed_user_group_membership",
        "fed_user_required_action", "fed_user_role_mapping", "federated_identity", "federated_user", "group_attribute", "group_role_mapping",
        "identity_provider", "identity_provider_config", "identity_provider_mapper", "idp_mapper_config", "keycloak_group", "keycloak_role",
        "migration_model", "offline_client_session", "offline_user_session", "policy_config", "protocol_mapper", "protocol_mapper_config", "realm",
        "realm_attribute", "realm_default_groups", "realm_enabled_event_types", "realm_events_listeners", "realm_localizations",
        "realm_required_credential", "realm_smtp_config", "realm_supported_locales", "redirect_uris", "required_action_config",
        "required_action_provider", "resource_attribute", "resource_policy", "resource_server", "resource_server_perm_ticket",
        "resource_server_policy", "resource_server_resource", "resource_server_scope", "resource_uris", "role_attribute", "scope_mapping",
        "scope_policy", "user_attribute", "user_consent", "user_consent_client_scope", "user_entity", "user_federation_config",
        "user_federation_mapper", "user_federation_mapper_config", "user_federation_provider", "user_group_membership", "user_required_action",
        "user_role_mapping", "user_session", "user_session_note", "username_login_failure", "web_origins");

    for (String tableName : tablesToDelete) {
      assertTrue("Table " + tableName + " should exist before dropping", tableExists(context, tableName));
    }

    List<String> tablesBeforeMigration = fetchAllTableNames(context);

    // Drop the tables
    V0_50_33_009__DropKeycloakTables.dropTables(context, tablesToDelete);

    // Verify that each table does not exist after the drop operation
    for (String tableName : tablesToDelete) {
      assertFalse("Table " + tableName + " should not exist after dropping", tableExists(context, tableName));
    }

    List<String> tablesAfterMigration = fetchAllTableNames(context);

    tablesBeforeMigration.removeAll(tablesToDelete);

    // Verify that no other tables except Keycloak tables were dropped
    assertEquals(tablesBeforeMigration, tablesAfterMigration);
  }

  private List<String> fetchAllTableNames(DSLContext ctx) {
    return ctx.meta().getTables().stream()
        .map(org.jooq.Table::getName)
        .collect(java.util.stream.Collectors.toList());
  }

  private boolean tableExists(final DSLContext context, String tableName) {
    return context.fetchExists(
        org.jooq.impl.DSL.select()
            .from("information_schema.tables")
            .where(DSL.field("table_name").eq(tableName)
                .and(DSL.field("table_schema").eq("public"))));
  }

}
