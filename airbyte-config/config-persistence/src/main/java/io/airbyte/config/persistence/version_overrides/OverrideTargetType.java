/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

/**
 * The type of target that a version override (ActorDefinitionVersionOverride) applies to.
 */
public enum OverrideTargetType {

  ACTOR("actor"),
  WORKSPACE("workspace");

  private final String name;

  OverrideTargetType(final String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

}
