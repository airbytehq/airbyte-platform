/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import java.io.File;

/**
 * This interface represents configuration objects used by Airbyte and Airbyte cloud.
 */
public interface AirbyteConfig {

  String name();

  /**
   * Get field id.
   *
   * @return the name of the field storing the id for the configuration object
   */
  String getIdFieldName();

  /**
   * Get id.
   *
   * @return the actual id of the configuration object
   */
  <T> String getId(T config);

  /**
   * Get config schema file.
   *
   * @return the path to the yaml file that defines the schema of the configuration object
   */
  File getConfigSchemaFile();

  <T> Class<T> getClassName();

}
