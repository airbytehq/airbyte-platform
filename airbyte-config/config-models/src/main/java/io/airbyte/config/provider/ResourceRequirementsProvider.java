/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.provider;

import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ResourceRequirementsType;
import java.util.Optional;

/**
 * ResourceRequirementsProvider abstraction.
 */
public interface ResourceRequirementsProvider {

  /**
   * Returns ResourceRequirements for a given type and subType.
   * <p>
   * Note that subType is treated as a hint, the expected behavior for the implementations is to
   * always return a valid config for a given ResourceRequirementsType and may provide a more specific
   * ResourceRequirements for a given subType.
   */
  ResourceRequirements getResourceRequirements(final ResourceRequirementsType type, final Optional<String> subType);

}
