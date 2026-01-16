/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

/**
 * Collection of constants used to identify and extract ID values from a raw HTTP request for
 * authentication purposes.
 */
object AuthenticationFields {
  /**
   * Name of the field in HTTP request bodies that contains the connection ID value.
   */
  const val CONNECTION_ID_FIELD_NAME: String = "connectionId"

  /**
   * Name of the field in HTTP request bodies that contains the connection IDs value.
   */
  const val CONNECTION_IDS_FIELD_NAME: String = "connectionIds"

  /**
   * Name of the field in HTTP request bodies that contains the destination ID value.
   */
  const val DESTINATION_ID_FIELD_NAME: String = "destinationId"

  /**
   * Name of the field in HTTP request bodies that contains the job ID value.
   */
  const val JOB_ID_FIELD_NAME: String = "id"

  /**
   * Alternative name of the field in HTTP request bodies that contains the job ID value.
   */
  const val JOB_ID_ALT_FIELD_NAME: String = "jobId"

  /**
   * Name of the field in HTTP request bodies that contains the operation ID value.
   */
  const val OPERATION_ID_FIELD_NAME: String = "operationId"

  /**
   * Name of the field in HTTP request bodies that contains the source ID value.
   */
  const val SOURCE_ID_FIELD_NAME: String = "sourceId"

  /**
   * Name of the field in HTTP request bodies that contains the source definition ID value.
   */
  const val SOURCE_DEFINITION_ID_FIELD_NAME: String = "sourceDefinitionId"

  /**
   * Name of the field in HTTP request bodies that contains the Airbyte-assigned user ID value.
   */
  const val AIRBYTE_USER_ID_FIELD_NAME: String = "userId"

  /**
   * Name of the field in HTTP request bodies that contains the resource creator's Airbyte-assigned
   * user ID value.
   */
  const val CREATOR_USER_ID_FIELD_NAME: String = "creatorUserId"

  /**
   * Name of the field in HTTP request bodies that contains the external auth ID value.
   */
  const val EXTERNAL_AUTH_ID_FIELD_NAME: String = "authUserId"

  /**
   * Name of the field in HTTP request bodies that contains the workspace ID value.
   */
  const val WORKSPACE_ID_FIELD_NAME: String = "workspaceId"

  /**
   * Name of the field in HTTP request bodies that contains the workspace IDs value.
   */
  const val WORKSPACE_IDS_FIELD_NAME: String = "workspaceIds"

  /**
   * Name of the field in HTTP request bodies that contains the config ID value - this is equivalent
   * to the connection ID.
   */
  const val CONFIG_ID_FIELD_NAME: String = "configId"

  const val ORGANIZATION_ID_FIELD_NAME: String = "organizationId"

  const val PERMISSION_ID_FIELD_NAME: String = "permissionId"

  const val SCOPE_TYPE_FIELD_NAME: String = "scopeType"

  const val SCOPE_ID_FIELD_NAME: String = "scopeId"

  /**
   * The dataplane and dataplane group APIs use snake case for their request body field names,
   * which is inconsistent with the rest of the API. Unfortunately, this is already part of the
   * public API interface, so for now we have to support it in order to avoid breaking changes.
   */
  const val DATAPLANE_GROUP_ID_SNAKE_CASE_FIELD_NAME: String = "dataplane_group_id"
  const val DATAPLANE_ID_SNAKE_CASE_FIELD_NAME: String = "dataplane_id"
  const val ORGANIZATION_ID_SNAKE_CASE_FIELD_NAME: String = "organization_id"
}
