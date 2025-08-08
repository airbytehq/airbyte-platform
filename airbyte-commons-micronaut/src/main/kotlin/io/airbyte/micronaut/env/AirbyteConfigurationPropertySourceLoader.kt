/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.env

import io.micronaut.context.env.ActiveEnvironment
import io.micronaut.context.env.PropertySource
import io.micronaut.context.env.PropertySourceLoader
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.context.exceptions.ConfigurationException
import io.micronaut.core.io.ResourceLoader
import java.io.InputStream
import java.net.URL
import java.util.Optional

private const val AIRBYTE_CONFIGURATION_FILE_PATH = "classpath:airbyte-configuration.yml"
private const val DEFAULT_POSITION = YamlPropertySourceLoader.DEFAULT_POSITION - 100
private const val NAME = "airbyte-configuration"

/**
 * Custom Micronaut [PropertySourceLoader] that is responsible for finding all airbyte-configuration.yml files
 * present on the classpath and combining them into a [PropertySource] with lower precedence than a service's
 * application.yml file or any externally loaded configuration (e.g. Java properties, Kubernetes configmaps, etc.).
 */
class AirbyteConfigurationPropertySourceLoader : PropertySourceLoader {
  private val yamlLoader = YamlPropertySourceLoader()

  override fun load(
    resourceName: String,
    resourceLoader: ResourceLoader,
  ): Optional<PropertySource> =
    try {
      Optional.of(
        PropertySource.of(
          NAME,
          loadConfiguration(resourceLoader).flatMap { it.entries }.associate { it.key to it.value },
          DEFAULT_POSITION,
        ),
      )
    } catch (e: Exception) {
      throw ConfigurationException("Could not load Airbyte configuration file(s).", e)
    }

  override fun loadEnv(
    resourceName: String,
    resourceLoader: ResourceLoader,
    activeEnvironment: ActiveEnvironment,
  ): Optional<PropertySource> = load(resourceName, resourceLoader)

  override fun read(
    name: String,
    input: InputStream,
  ): Map<String, Any> = yamlLoader.read(name, input)

  private fun loadConfiguration(resourceLoader: ResourceLoader) =
    resourceLoader
      .getResources(AIRBYTE_CONFIGURATION_FILE_PATH)
      .map { resource: URL -> read(resource.file, resource.openStream()) }
      .toList()
}
