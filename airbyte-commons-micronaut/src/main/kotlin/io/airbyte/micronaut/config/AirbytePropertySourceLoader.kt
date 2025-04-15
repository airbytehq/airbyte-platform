/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.config

import io.micronaut.context.env.ActiveEnvironment
import io.micronaut.context.env.PropertySource
import io.micronaut.context.env.PropertySourceLoader
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.context.exceptions.ConfigurationException
import io.micronaut.core.io.ResourceLoader
import io.micronaut.core.io.file.DefaultFileSystemResourceLoader
import java.io.IOException
import java.io.InputStream
import java.util.Optional
import kotlin.jvm.optionals.getOrNull

private const val AIRBYTE_YML_PATH = "/app/configs/airbyte.yml"
private const val AIRBYTE_YML_KEY = "airbyte-yml"

/**
 * Custom Micronaut {@link PropertySourceLoader} that loads the airbyte.yml file and prefixes all
 * properties within with "airbyte-yml.". Micronaut registers this property loader via the
 * resources/META-INF/services/io.micronaut.context.env.PropertySourceLoader file.
 */
class AirbytePropertySourceLoader : PropertySourceLoader {
  private val yamlLoader = YamlPropertySourceLoader()

  override fun load(
    resourceName: String,
    resourceLoader: ResourceLoader,
  ): Optional<PropertySource> {
    // load the airbyte.yml file as an InputStream, if it exists
    val airbyteConfigFile = getAirbyteInputStream() ?: return Optional.empty()

    try {
      airbyteConfigFile.use {
        val yaml = read(resourceName, it)

        // Prefix all configuration from that file with airbyte.
        val prefixedProps =
          yaml
            .map { (key, value) -> "$AIRBYTE_YML_KEY.$key" to value }
            .toMap()

        return Optional.of(PropertySource.of(prefixedProps))
      }
    } catch (e: IOException) {
      throw ConfigurationException("Could not load airbyte.yml configuration file.", e)
    }
  }

  override fun loadEnv(
    resourceName: String,
    resourceLoader: ResourceLoader,
    activeEnvironment: ActiveEnvironment,
  ): Optional<PropertySource> = load(resourceName, resourceLoader)

  @Throws(IOException::class)
  override fun read(
    name: String,
    input: InputStream,
  ): Map<String, Any> = yamlLoader.read(name, input)

  private fun getAirbyteInputStream(): InputStream? = DefaultFileSystemResourceLoader().getResourceAsStream(AIRBYTE_YML_PATH).getOrNull()
}
