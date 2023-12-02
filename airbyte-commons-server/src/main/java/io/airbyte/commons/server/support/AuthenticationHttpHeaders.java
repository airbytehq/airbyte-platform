/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.micronaut.http.HttpHeaders;

/**
 * Collection of HTTP headers that are used to perform authentication/authorization.
 */
public final class AuthenticationHttpHeaders {

  /**
   * Prefix that denotes an internal Airbyte header.
   */
  public static final String AIRBYTE_HEADER_PREFIX = "X-Airbyte-";

  /**
   * Authorization header.
   */
  public static final String AUTHORIZATION_HEADER = HttpHeaders.AUTHORIZATION;

  /**
   * HTTP header that contains the connection ID for authorization purposes.
   */
  public static final String CONNECTION_ID_HEADER = AIRBYTE_HEADER_PREFIX + "Connection-Id";
  public static final String CONNECTION_IDS_HEADER = AIRBYTE_HEADER_PREFIX + "Connection-Ids";

  /**
   * HTTP header that contains the destination ID for authorization purposes.
   */
  public static final String DESTINATION_ID_HEADER = AIRBYTE_HEADER_PREFIX + "Destination-Id";

  /**
   * HTTP header that contains the job ID for authorization purposes.
   */
  public static final String JOB_ID_HEADER = AIRBYTE_HEADER_PREFIX + "Job-Id";

  /**
   * HTTP header that contains the operation ID for authorization purposes.
   */
  public static final String OPERATION_ID_HEADER = AIRBYTE_HEADER_PREFIX + "Operation-Id";

  /**
   * HTTP header that contains the source ID for authorization purposes.
   */
  public static final String SOURCE_ID_HEADER = AIRBYTE_HEADER_PREFIX + "Source-Id";

  /**
   * HTTP header that contains the source definition ID for authorization purposes.
   */
  public static final String SOURCE_DEFINITION_ID_HEADER = AIRBYTE_HEADER_PREFIX + "Source-Definition-Id";

  /**
   * HTTP header that contains the Airbyte-assigned user ID for authorization purposes.
   */
  public static final String AIRBYTE_USER_ID_HEADER = AIRBYTE_HEADER_PREFIX + "User-Id";

  /**
   * HTTP header that contains the resource creator's Airbyte-assigned user ID for authorization
   * purposes.
   */
  public static final String CREATOR_USER_ID_HEADER = AIRBYTE_HEADER_PREFIX + "Creator-User-Id";

  /**
   * HTTP header that contains the external auth ID for authorization purposes.
   */
  public static final String EXTERNAL_AUTH_ID_HEADER = AIRBYTE_HEADER_PREFIX + "External-Auth-Id";

  /**
   * HTTP header that contains the auth user ID for authorization purposes.
   */
  public static final String EMAIL_HEADER = AIRBYTE_HEADER_PREFIX + "Email";

  /**
   * HTTP header that contains the workspace ID for authorization purposes.
   */
  public static final String WORKSPACE_ID_HEADER = AIRBYTE_HEADER_PREFIX + "Workspace-Id";
  public static final String WORKSPACE_IDS_HEADER = AIRBYTE_HEADER_PREFIX + "Workspace-Ids";
  public static final String CONFIG_ID_HEADER = AIRBYTE_HEADER_PREFIX + "Config-Id";

  public static final String ORGANIZATION_ID_HEADER = AIRBYTE_HEADER_PREFIX + "Organization-Id";
  public static final String PERMISSION_ID_HEADER = AIRBYTE_HEADER_PREFIX + "Permission-Id";

  private AuthenticationHttpHeaders() {}

}
