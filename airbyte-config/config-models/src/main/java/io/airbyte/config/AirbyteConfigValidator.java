/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import io.airbyte.validation.json.AbstractSchemaValidator;
import java.nio.file.Path;

/**
 * Implementation of validator for AirbyteConfig.
 */
public class AirbyteConfigValidator extends AbstractSchemaValidator<ConfigSchema> {

  public static final AirbyteConfigValidator AIRBYTE_CONFIG_VALIDATOR = new AirbyteConfigValidator();

  @Override
  public Path getSchemaPath(final ConfigSchema configType) {
    return configType.getConfigSchemaFile().toPath();
  }

}
