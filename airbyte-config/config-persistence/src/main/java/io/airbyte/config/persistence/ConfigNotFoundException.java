/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.config.AirbyteConfig;
import java.util.UUID;

/**
 * Exception when the requested config cannot be found.
 */
public class ConfigNotFoundException extends Exception {

  // This is a specific error type that is used when an organization cannot be found
  // from a given workspace. Workspaces will soon require an organization, so this
  // error is temporary and will be removed once the requirement is enforced.
  public static final String NO_ORGANIZATION_FOR_WORKSPACE = "NO_ORGANIZATION_FOR_WORKSPACE";

  private static final long serialVersionUID = 836273627;
  private final String type;
  private final String configId;

  public ConfigNotFoundException(final String type, final String configId) {
    super(String.format("config type: %s id: %s", type, configId));
    this.type = type;
    this.configId = configId;
  }

  public ConfigNotFoundException(final AirbyteConfig type, final String configId) {
    this(type.toString(), configId);
  }

  public ConfigNotFoundException(final AirbyteConfig type, final UUID uuid) {
    this(type.toString(), uuid.toString());
  }

  public String getType() {
    return type;
  }

  public String getConfigId() {
    return configId;
  }

}
