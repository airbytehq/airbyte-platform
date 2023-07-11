/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.helper;

import io.airbyte.commons.version.Version;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.micronaut.core.util.CollectionUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This should be a temporary class to assist during the transition from "normalization" containers
 * running after a destination container sync, to moving "normalization" into the destination.
 * container. "Normalization" will also be rebranded to "Typing" and "de-duping"
 */
public class NormalizationInDestinationHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationInDestinationHelper.class);

  private static final Map<String, String> IN_DESTINATION_NORMALIZATION_ENV_VAR = Map.of("NORMALIZATION_TECHNIQUE", "LEGACY");

  public static boolean normalizationStepRequired(final List<StandardSyncOperation> standardSyncOperations) {
    return CollectionUtils.isNotEmpty(standardSyncOperations)
        && standardSyncOperations.stream().anyMatch(op -> OperatorType.NORMALIZATION.equals(op.getOperatorType()));
  }

  public static Map<String, String> getAdditionalEnvironmentVariables(final boolean shouldNormalizeInDestination) {
    return shouldNormalizeInDestination ? IN_DESTINATION_NORMALIZATION_ENV_VAR : Collections.emptyMap();
  }

  /**
   * Whether this replication should normalize in the destination container.
   *
   * @param standardSyncOperations the sync operations for the replication job
   * @param containerName the name of the destination container
   * @param minSupportedVersion the minimum version that supports normalization for this destination,
   *        if this workspace has opted into the feature flag for normalization in destination
   *        containers (otherwise an empty string)
   * @return a boolean value of whether normalization should be run in the destination container
   */
  public static boolean shouldNormalizeInDestination(final List<StandardSyncOperation> standardSyncOperations,
                                                     final String containerName,
                                                     final String minSupportedVersion) {
    final var requiresNormalization = normalizationStepRequired(standardSyncOperations);
    final var normalizationSupported = connectorSupportsNormalizationInDestination(containerName, minSupportedVersion);
    LOGGER.info("Requires Normalization: {}, Normalization Supported: {}, Feature Flag Enabled: {}",
        requiresNormalization, normalizationSupported, !minSupportedVersion.isBlank());
    return requiresNormalization && normalizationSupported;
  }

  private static boolean connectorSupportsNormalizationInDestination(final String containerName,
                                                                     final String minSupportedVersion) {
    if (!minSupportedVersion.isBlank()) {
      return DockerImageNameHelper.extractImageVersion(containerName)
          .map(version -> version.greaterThanOrEqualTo(new Version(minSupportedVersion)))
          .orElse(false);
    }
    return false;
  }

}
