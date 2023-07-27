/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs;

import io.airbyte.config.ActorType;
import java.util.UUID;

/**
 * Exception when the requested registry definition cannot be found.
 */
public class RegistryDefinitionNotFoundException extends Exception {

  public RegistryDefinitionNotFoundException(final ActorType type, final String id) {
    super(String.format("type: %s definition / id: %s", type, id));
  }

  public RegistryDefinitionNotFoundException(final ActorType type, final UUID uuid) {
    this(type, uuid.toString());
  }

}
