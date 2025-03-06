/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Table schema.
 */
public interface TableSchema {

  /**
   * Get table name.
   *
   * @return table name in lower case
   */
  String getTableName();

  /**
   * Get table definition.
   *
   * @return the table definition in JsonSchema
   */
  JsonNode getTableDefinition();

}
