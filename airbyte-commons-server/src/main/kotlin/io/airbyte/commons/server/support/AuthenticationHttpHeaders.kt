/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.micronaut.http.HttpHeaders

/**
 * Collection of HTTP headers that are used to perform authentication/authorization.
 */
object AuthenticationHttpHeaders {
  /**
   * Prefix that denotes an internal Airbyte header.
   */
  const val AIRBYTE_HEADER_PREFIX: String = "X-Airbyte-"

  /**
   * Authorization header.
   */
  const val AUTHORIZATION_HEADER: String = HttpHeaders.AUTHORIZATION

  /**
   * HTTP header that contains the connection ID for authorization purposes.
   */
  const val CONNECTION_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Connection-Id"
  const val CONNECTION_IDS_HEADER: String = AIRBYTE_HEADER_PREFIX + "Connection-Ids"

  /**
   * HTTP header that contains the destination ID for authorization purposes.
   */
  const val DESTINATION_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Destination-Id"

  /**
   * HTTP header that contains the job ID for authorization purposes.
   */
  const val JOB_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Job-Id"

  /**
   * HTTP header that contains the operation ID for authorization purposes.
   */
  const val OPERATION_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Operation-Id"

  /**
   * HTTP header that contains the source ID for authorization purposes.
   */
  const val SOURCE_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Source-Id"

  /**
   * HTTP header that contains the source definition ID for authorization purposes.
   */
  const val SOURCE_DEFINITION_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Source-Definition-Id"

  /**
   * HTTP header that contains the Airbyte-assigned user ID for authorization purposes.
   */
  const val AIRBYTE_USER_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "User-Id"

  /**
   * HTTP header that contains the resource creator's Airbyte-assigned user ID for authorization
   * purposes.
   */
  const val CREATOR_USER_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Creator-User-Id"

  /**
   * HTTP header that contains the external auth ID for authorization purposes.
   */
  const val EXTERNAL_AUTH_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "External-Auth-Id"

  /**
   * HTTP header that contains the workspace ID for authorization purposes.
   */
  const val WORKSPACE_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Workspace-Id"
  const val WORKSPACE_IDS_HEADER: String = AIRBYTE_HEADER_PREFIX + "Workspace-Ids"
  const val CONFIG_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Config-Id"

  const val ORGANIZATION_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Organization-Id"
  const val PERMISSION_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Permission-Id"
  const val IS_PUBLIC_API_HEADER: String = AIRBYTE_HEADER_PREFIX + "Is-Public-Api"

  /**
   * HTTP header that contains the scope type (ie 'workspace' or 'organization') for authorization
   * purposes.
   */
  const val SCOPE_TYPE_HEADER: String = AIRBYTE_HEADER_PREFIX + "Scope-Type"

  /**
   * HTTP header that contains the scope ID for authorization purposes.
   */
  const val SCOPE_ID_HEADER: String = AIRBYTE_HEADER_PREFIX + "Scope-Id"
}
