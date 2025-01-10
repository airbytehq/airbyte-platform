/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tables in the config db.
 */
public enum ConfigsDatabaseTables {

  ACTIVE_DECLARATIVE_MANIFEST,
  ACTOR,
  ACTOR_DEFINITION,
  ACTOR_DEFINITION_CONFIG_INJECTION,
  ACTOR_OAUTH_PARAMETER,
  CONNECTION,
  CONNECTION_OPERATION,
  DECLARATIVE_MANIFEST,
  OPERATION,
  STATE,
  WORKSPACE,
  USER,
  PERMISSION,
  CONNECTOR_BUILDER_PROJECT;

  /**
   * Get table name.
   *
   * @return name of table
   */
  public String getTableName() {
    return name().toLowerCase();
  }

  /**
   * Get table names.
   *
   * @return table names in lower case
   */
  public static Set<String> getTableNames() {
    return Stream.of(ConfigsDatabaseTables.values()).map(ConfigsDatabaseTables::getTableName).collect(Collectors.toSet());
  }

}
