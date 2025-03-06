/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.provider;

import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ResourceRequirementsType;
import java.util.Optional;

/**
 * ResourceRequirementsProvider abstraction.
 * <p>
 * We are classifying resources with a type and a subType. The type being one of Source, Destination
 * and Orchestrator. For a Source, the subType is one of (api, database, file, custom). The subType
 * for destinations and orchestrators is inherited from the source of the same connection. The
 * reason is that the source sets the expectations related to the throughput of a sync. For example,
 * an API source is most likely going to be slower than a database source and won't be able to
 * leverage as much CPU. Similarly, Orchestrators and Destinations should have a load dependent on
 * the throughput from the source.
 * <p>
 * The notion of variant is to enable global classes on top of the subType. For example, we could
 * imagine a low/high resources model based on the urgency of a job. Variant is here to open this
 * possibility.
 */
public interface ResourceRequirementsProvider {

  String DEFAULT_VARIANT = "default";

  /**
   * Returns ResourceRequirements for a given type and subType.
   * <p>
   * Note that subType is treated as a hint, the expected behavior for the implementations is to
   * always return a valid config for a given ResourceRequirementsType and may provide a more specific
   * ResourceRequirements for a given subType.
   */
  ResourceRequirements getResourceRequirements(final ResourceRequirementsType type, final Optional<String> subType);

  /**
   * Returns ResourceRequirements for a given type, subType and variant.
   * <p>
   * Note that subType is treated as a hint, the expected behavior for the implementations is to
   * always return a valid config for a given ResourceRequirementsType and may provide a more specific
   * ResourceRequirements for a given subType.
   * <p>
   * Variant will be preferred if defined, otherwise, it falls back to default.
   */
  ResourceRequirements getResourceRequirements(final ResourceRequirementsType type, final Optional<String> subType, final String variant);

}
