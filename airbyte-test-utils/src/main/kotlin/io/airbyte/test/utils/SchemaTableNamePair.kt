/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

/**
 * Schema table and name pair.
 *
 * @param schemaName schema name
 * @param tableName table name
 */
data class SchemaTableNamePair(
  @JvmField val schemaName: String,
  @JvmField val tableName: String,
) {
  fun getFullyQualifiedTableName(): String = "$schemaName.$tableName"
}
