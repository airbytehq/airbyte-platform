/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

/**
 * Enumeration of the ID values that are used to perform authentication. These values are used to
 * fetch roles associated with an authenticated user.
 */
enum class AuthenticationId(
  @JvmField val fieldName: String,
  @JvmField val httpHeader: String,
) {
  EXTERNAL_AUTH_ID(AuthenticationFields.EXTERNAL_AUTH_ID_FIELD_NAME, AuthenticationHttpHeaders.EXTERNAL_AUTH_ID_HEADER),
  CONNECTION_ID(AuthenticationFields.CONNECTION_ID_FIELD_NAME, AuthenticationHttpHeaders.CONNECTION_ID_HEADER),
  CONNECTION_IDS(AuthenticationFields.CONNECTION_IDS_FIELD_NAME, AuthenticationHttpHeaders.CONNECTION_IDS_HEADER),

  DESTINATION_ID_(AuthenticationFields.DESTINATION_ID_FIELD_NAME, AuthenticationHttpHeaders.DESTINATION_ID_HEADER),
  JOB_ID(AuthenticationFields.JOB_ID_FIELD_NAME, AuthenticationHttpHeaders.JOB_ID_HEADER),
  JOB_ID_ALT(AuthenticationFields.JOB_ID_ALT_FIELD_NAME, AuthenticationHttpHeaders.JOB_ID_HEADER),
  OPERATION_ID(AuthenticationFields.OPERATION_ID_FIELD_NAME, AuthenticationHttpHeaders.OPERATION_ID_HEADER),
  SOURCE_ID(AuthenticationFields.SOURCE_ID_FIELD_NAME, AuthenticationHttpHeaders.SOURCE_ID_HEADER),
  SOURCE_DEFINITION_ID(AuthenticationFields.SOURCE_DEFINITION_ID_FIELD_NAME, AuthenticationHttpHeaders.SOURCE_DEFINITION_ID_HEADER),
  AIRBYTE_USER_ID(AuthenticationFields.AIRBYTE_USER_ID_FIELD_NAME, AuthenticationHttpHeaders.AIRBYTE_USER_ID_HEADER),
  CREATOR_USER_ID(AuthenticationFields.CREATOR_USER_ID_FIELD_NAME, AuthenticationHttpHeaders.CREATOR_USER_ID_HEADER),
  WORKSPACE_ID(AuthenticationFields.WORKSPACE_ID_FIELD_NAME, AuthenticationHttpHeaders.WORKSPACE_ID_HEADER),
  WORKSPACE_IDS(AuthenticationFields.WORKSPACE_IDS_FIELD_NAME, AuthenticationHttpHeaders.WORKSPACE_IDS_HEADER),
  CONFIG_ID(AuthenticationFields.CONFIG_ID_FIELD_NAME, AuthenticationHttpHeaders.CONFIG_ID_HEADER),
  ORGANIZATION_ID(AuthenticationFields.ORGANIZATION_ID_FIELD_NAME, AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER),
  PERMISSION_ID(AuthenticationFields.PERMISSION_ID_FIELD_NAME, AuthenticationHttpHeaders.PERMISSION_ID_HEADER),
  SCOPE_TYPE(AuthenticationFields.SCOPE_TYPE_FIELD_NAME, AuthenticationHttpHeaders.SCOPE_TYPE_HEADER),
  SCOPE_ID(AuthenticationFields.SCOPE_ID_FIELD_NAME, AuthenticationHttpHeaders.SCOPE_ID_HEADER),
}
