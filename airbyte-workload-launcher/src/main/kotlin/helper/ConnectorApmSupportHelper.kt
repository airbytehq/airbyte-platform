/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.helper

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.constants.WorkerConstants
import io.fabric8.kubernetes.api.model.EnvVar
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

private const val IMAGE_DELIMITER = ":"

/**
 * Utility class for Connector level APM support helper.
 */
@Singleton
class ConnectorApmSupportHelper {
  /**
   * addServerNameAndVersionToEnvVars.
   *
   * @param imageNameAndVersion ConnectorNameAndVersion record.
   * @param envVars List of environment variables.
   */
  fun addServerNameAndVersionToEnvVars(
    imageNameAndVersion: String?,
    envVars: MutableList<EnvVar>?,
  ) {
    val serviceName = getImageName(imageNameAndVersion)
    val version = getImageVersion(imageNameAndVersion)

    envVars?.let { _ ->
      envVars.add(EnvVar(io.airbyte.commons.envvar.EnvVar.DD_SERVICE.name, serviceName, null))
      envVars.add(EnvVar(io.airbyte.commons.envvar.EnvVar.DD_VERSION.name, version, null))
      logger.debug { "Added service name $serviceName and version $version to env vars for Datadog" }
    }
  }

  /**
   * addDatadogVars.
   *
   * @param envVars List of environment variables.
   */
  fun addApmEnvVars(envVars: MutableList<EnvVar>) {
    val env: MutableMap<String?, String?> = getEnv()

    envVars.add(EnvVar(io.airbyte.commons.envvar.EnvVar.JAVA_OPTS.name, WorkerConstants.DD_ENV_VAR, null))
    if (env.containsKey(io.airbyte.commons.envvar.EnvVar.DD_AGENT_HOST.name)) {
      envVars.add(
        EnvVar(io.airbyte.commons.envvar.EnvVar.DD_AGENT_HOST.name, env.get(io.airbyte.commons.envvar.EnvVar.DD_AGENT_HOST.name), null),
      )
    }
    if (env.containsKey(io.airbyte.commons.envvar.EnvVar.DD_DOGSTATSD_PORT.name)) {
      envVars.add(
        EnvVar(
          io.airbyte.commons.envvar.EnvVar.DD_DOGSTATSD_PORT.name,
          env.get(io.airbyte.commons.envvar.EnvVar.DD_DOGSTATSD_PORT.name),
          null,
        ),
      )
    }
  }

  @VisibleForTesting
  fun getEnv(): MutableMap<String?, String?> = System.getenv()

  /**
   * Extracts the image name from the provided image string, if the image string uses the following
   * format: `<image name>:<image version>`. Image name may include registry and port number,
   * which is also delimited by ":"
   *
   * @param image The image.
   * @return The name extracted from the image, or the originally provided string if blank.
   */
  fun getImageName(image: String?): String? {
    if (image?.isNotBlank() == true) {
      val delimeterIndex = image.lastIndexOf(IMAGE_DELIMITER)
      if (delimeterIndex >= 0) {
        return image.substring(0, delimeterIndex)
      }
    }
    // If image is null, empty, or does not contain a delimiter, return the original string.
    return image
  }

  /**
   * Extracts the image version from the provided image string, if the image string uses the following
   * format: `<image name>:<image version>`. Image name may include registry and port number,
   * which is also delimited by ":"
   *
   * @param image The image.
   * @return The version extracted from the image, or the originally provided string if blank.
   */
  fun getImageVersion(image: String?): String? {
    if (image?.isNotBlank() == true) {
      val delimeterIndex = image.lastIndexOf(IMAGE_DELIMITER)
      if (delimeterIndex >= 0 && image.length > delimeterIndex + 1) {
        return image.substring(delimeterIndex + 1)
      }
    }
    // If image is null, empty, or does not contain a delimiter, return the original string.
    return image
  }
}
