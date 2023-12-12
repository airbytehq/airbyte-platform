/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

/**
 * Collection of constants used to identify and extract ID values from a raw HTTP request for
 * authentication purposes.
 */
public final class AuthenticationFields {

  /**
   * Name of the field in HTTP request bodies that contains the connection ID value.
   */
  public static final String CONNECTION_ID_FIELD_NAME = "connectionId";

  /**
   * Name of the field in HTTP request bodies that contains the connection IDs value.
   */
  public static final String CONNECTION_IDS_FIELD_NAME = "connectionIds";

  /**
   * Name of the field in HTTP request bodies that contains the destination ID value.
   */
  public static final String DESTINATION_ID_FIELD_NAME = "destinationId";

  /**
   * Name of the field in HTTP request bodies that contains the job ID value.
   */
  public static final String JOB_ID_FIELD_NAME = "id";

  /**
   * Alternative name of the field in HTTP request bodies that contains the job ID value.
   */
  public static final String JOB_ID_ALT_FIELD_NAME = "jobId";

  /**
   * Name of the field in HTTP request bodies that contains the operation ID value.
   */
  public static final String OPERATION_ID_FIELD_NAME = "operationId";

  /**
   * Name of the field in HTTP request bodies that contains the source ID value.
   */
  public static final String SOURCE_ID_FIELD_NAME = "sourceId";

  /**
   * Name of the field in HTTP request bodies that contains the source definition ID value.
   */
  public static final String SOURCE_DEFINITION_ID_FIELD_NAME = "sourceDefinitionId";

  /**
   * Name of the field in HTTP request bodies that contains the Airbyte-assigned user ID value.
   */
  public static final String AIRBYTE_USER_ID_FIELD_NAME = "userId";

  /**
   * Name of the field in HTTP request bodies that contains the resource creator's Airbyte-assigned
   * user ID value.
   */
  public static final String CREATOR_USER_ID_FIELD_NAME = "creatorUserId";

  /**
   * Name of the field in HTTP request bodies that contains the external auth ID value.
   */
  public static final String EXTERNAL_AUTH_ID_FIELD_NAME = "authUserId";

  /**
   * Name of the field in HTTP request bodies that contains the email value.
   */
  public static final String EMAIL_FIELD_NAME = "email";

  /**
   * Name of the field in HTTP request bodies that contains the workspace ID value.
   */
  public static final String WORKSPACE_ID_FIELD_NAME = "workspaceId";

  /**
   * Name of the field in HTTP request bodies that contains the workspace IDs value.
   */
  public static final String WORKSPACE_IDS_FIELD_NAME = "workspaceIds";

  /**
   * Name of the field in HTTP request bodies that contains the config ID value - this is equivalent
   * to the connection ID.
   */
  public static final String CONFIG_ID_FIELD_NAME = "configId";

  public static final String ORGANIZATION_ID_FIELD_NAME = "organizationId";

  public static final String PERMISSION_ID_FIELD_NAME = "permissionId";

  private AuthenticationFields() {}

}
