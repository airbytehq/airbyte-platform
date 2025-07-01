/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.legacy

import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConnectorBuilderProject
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.SourceConnection
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardSyncState
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.User

/**
 * Config db schema.
 */
@Deprecated("Only remains to support older migrations. Delete once pre 1.0 migrations are deleted.")
enum class ConfigSchema(
  className: Class<*>,
  val idFieldName: String,
) {
  // user
  USER(
    User::class.java,
    "userId",
  ),

  // permission
  PERMISSION(
    Permission::class.java,
    "permissionId",
  ),

  // workspace
  STANDARD_WORKSPACE(
    StandardWorkspace::class.java,
    "workspaceId",
  ),

  // organization
  ORGANIZATION(
    Organization::class.java,
    "organizationId",
  ),

  // connector builder project
  CONNECTOR_BUILDER_PROJECT(
    ConnectorBuilderProject::class.java,
    "builderProjectId",
  ),

  // actor definition version
  ACTOR_DEFINITION_VERSION(
    ActorDefinitionVersion::class.java,
    "versionId",
  ),

  // source
  STANDARD_SOURCE_DEFINITION(
    StandardSourceDefinition::class.java,
    "sourceDefinitionId",
  ),
  SOURCE_CONNECTION(
    SourceConnection::class.java,
    "sourceId",
  ),

  // destination
  STANDARD_DESTINATION_DEFINITION(
    StandardDestinationDefinition::class.java,
    "destinationDefinitionId",
  ),
  DESTINATION_CONNECTION(
    DestinationConnection::class.java,
    "destinationId",
  ),

  // sync (i.e. connection)
  STANDARD_SYNC(
    StandardSync::class.java,
    "connectionId",
  ),
  STANDARD_SYNC_OPERATION(
    StandardSyncOperation::class.java,
    "operationId",
  ),
  STANDARD_SYNC_STATE(
    StandardSyncState::class.java,
    "connectionId",
  ),

  SOURCE_OAUTH_PARAM(
    SourceOAuthParameter::class.java,
    "oauthParameterId",
  ),
  DESTINATION_OAUTH_PARAM(
    DestinationOAuthParameter::class.java,
    "oauthParameterId",
  ),
}
