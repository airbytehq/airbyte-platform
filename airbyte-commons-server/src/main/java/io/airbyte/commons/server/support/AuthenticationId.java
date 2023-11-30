/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.AIRBYTE_USER_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONFIG_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_IDS_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.CREATOR_USER_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.DESTINATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.EMAIL_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.EXTERNAL_AUTH_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.OPERATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.PERMISSION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_DEFINITION_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_ID_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_IDS_HEADER;
import static io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER;

/**
 * Enumeration of the ID values that are used to perform authentication. These values are used to
 * fetch roles associated with an authenticated user.
 */
public enum AuthenticationId {

  EXTERNAL_AUTH_ID(AuthenticationFields.EXTERNAL_AUTH_ID_FIELD_NAME, EXTERNAL_AUTH_ID_HEADER),
  CONNECTION_ID(AuthenticationFields.CONNECTION_ID_FIELD_NAME, CONNECTION_ID_HEADER),
  CONNECTION_IDS(AuthenticationFields.CONNECTION_IDS_FIELD_NAME, CONNECTION_IDS_HEADER),

  DESTINATION_ID_(AuthenticationFields.DESTINATION_ID_FIELD_NAME, DESTINATION_ID_HEADER),
  EMAIL(AuthenticationFields.EMAIL_FIELD_NAME, EMAIL_HEADER),
  JOB_ID(AuthenticationFields.JOB_ID_FIELD_NAME, JOB_ID_HEADER),
  JOB_ID_ALT(AuthenticationFields.JOB_ID_ALT_FIELD_NAME, JOB_ID_HEADER),
  OPERATION_ID(AuthenticationFields.OPERATION_ID_FIELD_NAME, OPERATION_ID_HEADER),
  SOURCE_ID(AuthenticationFields.SOURCE_ID_FIELD_NAME, SOURCE_ID_HEADER),
  SOURCE_DEFINITION_ID(AuthenticationFields.SOURCE_DEFINITION_ID_FIELD_NAME, SOURCE_DEFINITION_ID_HEADER),
  AIRBYTE_USER_ID(AuthenticationFields.AIRBYTE_USER_ID_FIELD_NAME, AIRBYTE_USER_ID_HEADER),
  CREATOR_USER_ID(AuthenticationFields.CREATOR_USER_ID_FIELD_NAME, CREATOR_USER_ID_HEADER),
  WORKSPACE_ID(AuthenticationFields.WORKSPACE_ID_FIELD_NAME, WORKSPACE_ID_HEADER),
  WORKSPACE_IDS(AuthenticationFields.WORKSPACE_IDS_FIELD_NAME, WORKSPACE_IDS_HEADER),
  CONFIG_ID(AuthenticationFields.CONFIG_ID_FIELD_NAME, CONFIG_ID_HEADER),
  ORGANIZATION_ID(AuthenticationFields.ORGANIZATION_ID_FIELD_NAME, ORGANIZATION_ID_HEADER),
  PERMISSION_ID(AuthenticationFields.PERMISSION_ID_FIELD_NAME, PERMISSION_ID_HEADER);

  private final String fieldName;
  private final String httpHeader;

  AuthenticationId(final String fieldName, final String httpHeader) {
    this.fieldName = fieldName;
    this.httpHeader = httpHeader;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getHttpHeader() {
    return httpHeader;
  }

}
