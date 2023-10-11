/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import com.google.api.client.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.config.EnvConfigs;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.micrometer.common.util.StringUtils;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for Connector level APM support helper.
 */
public class ConnectorApmSupportHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorApmSupportHelper.class);

  @VisibleForTesting
  static final String JAVA_OPTS = "JAVA_OPTS";
  @VisibleForTesting
  static final String DD_SERVICE = "DD_SERVICE";
  @VisibleForTesting
  static final String DD_VERSION = "DD_VERSION";
  private static final String IMAGE_DELIMITER = ":";

  /**
   * addServerNameAndVersionToEnvVars.
   *
   * @param imageNameAndVersion ConnectorNameAndVersion record.
   * @param envVars List of environment variables.
   */
  public void addServerNameAndVersionToEnvVars(@NotNull final String imageNameAndVersion, @NotNull final List<EnvVar> envVars) {
    Preconditions.checkNotNull(imageNameAndVersion);
    Preconditions.checkNotNull(envVars);

    final String serviceName = getImageName(imageNameAndVersion);
    final String version = getImageVersion(imageNameAndVersion);

    envVars.add(new EnvVar(DD_SERVICE, serviceName, null));
    envVars.add(new EnvVar(DD_VERSION, version, null));
    LOGGER.debug("Added service name {} and version {} to env vars for Datadog", serviceName, version);
  }

  /**
   * addDatadogVars.
   *
   * @param envVars List of environment variables.
   */
  public void addApmEnvVars(final List<EnvVar> envVars) {
    final Map<String, String> env = getEnv();

    envVars.add(new EnvVar(JAVA_OPTS, WorkerConstants.DD_ENV_VAR, null));
    if (env.containsKey(EnvConfigs.DD_AGENT_HOST)) {
      envVars.add(new EnvVar(EnvConfigs.DD_AGENT_HOST, env.get(EnvConfigs.DD_AGENT_HOST), null));
    }
    if (env.containsKey(EnvConfigs.DD_DOGSTATSD_PORT)) {
      envVars.add(new EnvVar(EnvConfigs.DD_DOGSTATSD_PORT, env.get(EnvConfigs.DD_DOGSTATSD_PORT), null));
    }
  }

  @VisibleForTesting
  Map<String, String> getEnv() {
    return System.getenv();
  }

  /**
   * Extracts the image name from the provided image string, if the image string uses the following
   * format: {@code <image name>:<image version>}.
   *
   * @param image The image.
   * @return The name extracted from the image, or the originally provided string if blank.
   */
  public static String getImageName(final String image) {
    if (StringUtils.isNotEmpty(image)) {
      return image.split(IMAGE_DELIMITER)[0];
    }

    return image;
  }

  /**
   * Extracts the image version from the provided image string, if the image string uses the following
   * format: {@code <image name>:<image version>}.
   *
   * @param image The image.
   * @return The version extracted from the image, or the originally provided string if blank.
   */
  public static String getImageVersion(final String image) {
    if (StringUtils.isNotEmpty(image)) {
      return image.split(IMAGE_DELIMITER)[1];
    }

    return image;
  }

}
