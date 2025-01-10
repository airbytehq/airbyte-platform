/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import com.google.api.client.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.constants.WorkerConstants;
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

    envVars.add(new EnvVar(io.airbyte.commons.envvar.EnvVar.DD_SERVICE.name(), serviceName, null));
    envVars.add(new EnvVar(io.airbyte.commons.envvar.EnvVar.DD_VERSION.name(), version, null));
    LOGGER.debug("Added service name {} and version {} to env vars for Datadog", serviceName, version);
  }

  /**
   * addDatadogVars.
   *
   * @param envVars List of environment variables.
   */
  public void addApmEnvVars(final List<EnvVar> envVars) {
    final Map<String, String> env = getEnv();

    envVars.add(new EnvVar(io.airbyte.commons.envvar.EnvVar.JAVA_OPTS.name(), WorkerConstants.DD_ENV_VAR, null));
    if (env.containsKey(io.airbyte.commons.envvar.EnvVar.DD_AGENT_HOST.name())) {
      envVars.add(
          new EnvVar(io.airbyte.commons.envvar.EnvVar.DD_AGENT_HOST.name(), env.get(io.airbyte.commons.envvar.EnvVar.DD_AGENT_HOST.name()), null));
    }
    if (env.containsKey(io.airbyte.commons.envvar.EnvVar.DD_DOGSTATSD_PORT.name())) {
      envVars.add(new EnvVar(io.airbyte.commons.envvar.EnvVar.DD_DOGSTATSD_PORT.name(),
          env.get(io.airbyte.commons.envvar.EnvVar.DD_DOGSTATSD_PORT.name()), null));
    }
  }

  @VisibleForTesting
  Map<String, String> getEnv() {
    return System.getenv();
  }

  /**
   * Extracts the image name from the provided image string, if the image string uses the following
   * format: {@code <image name>:<image version>}. Image name may include registry and port number,
   * which is also delimited by ":"
   *
   * @param image The image.
   * @return The name extracted from the image, or the originally provided string if blank.
   */
  public static String getImageName(final String image) {
    if (StringUtils.isNotEmpty(image)) {
      final int delimeterIndex = image.lastIndexOf(IMAGE_DELIMITER);
      if (delimeterIndex >= 0) {
        return image.substring(0, delimeterIndex);
      }
    }
    // If image is null, empty, or does not contain a delimiter, return the original string.
    return image;
  }

  /**
   * Extracts the image version from the provided image string, if the image string uses the following
   * format: {@code <image name>:<image version>}. Image name may include registry and port number,
   * which is also delimited by ":"
   *
   * @param image The image.
   * @return The version extracted from the image, or the originally provided string if blank.
   */
  public static String getImageVersion(final String image) {
    if (StringUtils.isNotEmpty(image)) {
      final int delimeterIndex = image.lastIndexOf(IMAGE_DELIMITER);
      if (delimeterIndex >= 0 && image.length() > delimeterIndex + 1) {
        return image.substring(delimeterIndex + 1);
      }
    }
    // If image is null, empty, or does not contain a delimiter, return the original string.
    return image;
  }

}
