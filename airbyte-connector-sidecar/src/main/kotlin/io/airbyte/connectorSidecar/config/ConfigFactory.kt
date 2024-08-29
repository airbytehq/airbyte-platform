package io.airbyte.connectorSidecar.config

import io.airbyte.workers.pod.FileConstants
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path

@Factory
class ConfigFactory {
  /**
   * Returns the config directory which contains all the configuration files.
   *
   * @param configDir optional directory, defaults to FileConstants.CONFIG_DIR if not defined.
   * @return Configuration directory.
   */
  @Singleton
  @Named("configDir")
  fun configDir(
    @Value("\${airbyte.config-dir}") configDir: String?,
  ): String {
    if (configDir == null) {
      return FileConstants.CONFIG_DIR
    }
    return configDir
  }

  /**
   * Returns the path of where the connector output is being stored
   */
  @Singleton
  @Named("output")
  fun output(): Path {
    return Path.of(FileConstants.JOB_OUTPUT_FILE)
  }
}
