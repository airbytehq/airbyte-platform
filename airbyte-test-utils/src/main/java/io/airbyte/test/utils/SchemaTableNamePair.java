/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

/**
 * Schema table and name pair.
 *
 * @param schemaName schema name
 * @param tableName table name
 */
public record SchemaTableNamePair(String schemaName, String tableName) {

  public String getFullyQualifiedTableName() {
    return schemaName + "." + tableName;
  }

}
