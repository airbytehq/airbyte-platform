/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import com.google.api.client.util.Preconditions;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.EnvConfigs;
import io.airbyte.workers.WorkerConstants;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.micrometer.common.util.StringUtils;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for Connector level Datadog support helper.
 */
public class ConnectorDatadogSupportHelper {

  private static final String JAVA_OPTS = "JAVA_OPTS";
  private static final String DD_SERVICE = "DD_SERVICE";
  private static final String DD_VERSION = "DD_VERSION";

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorDatadogSupportHelper.class);

  /**
   * addServerNameAndVersionToEnvVars.
   *
   * @param imageNameAndVersion ConnectorNameAndVersion record.
   * @param envVars List of environment variables.
   */
  public void addServerNameAndVersionToEnvVars(final String imageNameAndVersion, final List<EnvVar> envVars) {
    Preconditions.checkNotNull(imageNameAndVersion);
    Preconditions.checkNotNull(envVars);

    final Optional<ImmutablePair<String, AirbyteVersion>> imageNameVersionPair = extractAirbyteVersionFromImageName(imageNameAndVersion, ":");
    if (imageNameVersionPair.isPresent()) {
      envVars.add(new EnvVar(DD_SERVICE, imageNameVersionPair.get().left, null));
      envVars.add(new EnvVar(DD_VERSION, imageNameVersionPair.get().right.serialize(), null));
    }
  }

  /**
   * getImageNameAndVersion.
   *
   * @param imageName image name in string format.
   * @param delimiter delimiter seperating connector image name and version.
   * @return ConnectorNameAndVersion Parsed ConnectorNameAndVersion record.
   */
  public Optional<ImmutablePair<String, AirbyteVersion>> extractAirbyteVersionFromImageName(final String imageName, final String delimiter) {
    Preconditions.checkNotNull(imageName);
    Preconditions.checkNotNull(delimiter);

    final String[] imageNameAndVersion = imageName.split(delimiter);
    final int expectedCount = 2;
    if (imageNameAndVersion.length == expectedCount && StringUtils.isNotEmpty(imageNameAndVersion[0])) {
      try {
        // custom connectors version number does not confirm to Airbyte version
        return Optional.of(ImmutablePair.of(imageNameAndVersion[0], new AirbyteVersion(imageNameAndVersion[1])));
      } catch (Exception ex) {
        // logged as info because we allow processing to continue
        LOGGER.info("error while extracting version from image: {}. Message: {}", imageName, ex.getMessage());
      }
    }
    return Optional.empty();
  }

  /**
   * addDatadogVars.
   *
   * @param envVars List of environment variables.
   */
  public void addDatadogVars(final List<EnvVar> envVars) {
    envVars.add(new EnvVar(JAVA_OPTS, WorkerConstants.DD_ENV_VAR, null));

    if (System.getenv(EnvConfigs.DD_AGENT_HOST) != null) {
      envVars.add(new EnvVar(EnvConfigs.DD_AGENT_HOST, System.getenv(EnvConfigs.DD_AGENT_HOST), null));
    }
    if (System.getenv(EnvConfigs.DD_DOGSTATSD_PORT) != null) {
      envVars.add(new EnvVar(EnvConfigs.DD_DOGSTATSD_PORT, System.getenv(EnvConfigs.DD_DOGSTATSD_PORT), null));
    }
  }

  /**
   * Utility function to check if connector image supports Datadog.
   *
   * @param firstConnectorVersionWithDatadog image and version when Datadog support was first added.
   * @param currentConnectorVersion current image and version number
   * @return True if current image version has Datadog support.
   */
  public boolean connectorVersionCompare(final String firstConnectorVersionWithDatadog, final String currentConnectorVersion) {
    final Optional<ImmutablePair<String, AirbyteVersion>> firstSupportedVersion =
        extractAirbyteVersionFromImageName(firstConnectorVersionWithDatadog, "=");
    final Optional<ImmutablePair<String, AirbyteVersion>> currentVersion = extractAirbyteVersionFromImageName(currentConnectorVersion, ":");

    return firstSupportedVersion.isPresent()
        && currentVersion.isPresent()
        && currentVersion.get().left.compareTo(firstSupportedVersion.get().left) == 0
        && currentVersion.get().right.greaterThanOrEqualTo(firstSupportedVersion.get().right);
  }

}
