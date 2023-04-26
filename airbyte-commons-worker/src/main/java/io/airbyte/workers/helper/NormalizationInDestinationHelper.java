/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

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
  private static final Version BIG_QUERY_IN_DESTINATION_MIN_VERSION = new Version("1.3.1");

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
   * @param featureFlagEnabled whether this workspace has opted into the feature flag for
   *        normalization in destination containers
   * @return a boolean value of whether normalization should be run in the destination container
   */
  public static boolean shouldNormalizeInDestination(final List<StandardSyncOperation> standardSyncOperations,
                                                     final String containerName,
                                                     final boolean featureFlagEnabled) {
    final var requiresNormalization = normalizationStepRequired(standardSyncOperations);
    final var normalizationSupported = connectorSupportsNormalizationInDestination(containerName);
    LOGGER.info("Requires Normalization: {} Normalization Supported: {}, Feature Flag Enabled: {}",
        requiresNormalization, normalizationSupported, featureFlagEnabled);
    return requiresNormalization && normalizationSupported && featureFlagEnabled;
  }

  private static boolean connectorSupportsNormalizationInDestination(final String containerName) {
    final var meetsMinVersion =
        DockerImageNameHelper.extractImageVersion(containerName)
            .map(version -> version.greaterThanOrEqualTo(BIG_QUERY_IN_DESTINATION_MIN_VERSION))
            .orElse(false);
    return DockerImageNameHelper.extractShortImageName(containerName).startsWith("destination-bigquery") && meetsMinVersion;
  }

}
