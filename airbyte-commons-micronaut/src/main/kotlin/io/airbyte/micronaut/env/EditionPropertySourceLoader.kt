/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.env

import io.micronaut.context.env.PropertySource
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.core.io.ResourceLoader
import java.util.Optional

/**
 * Loads properties based on the edition of Airbyte.
 * Looks for a file named `application-edition-<edition>.yml` where `<edition>` is the value of the
 * `AIRBYTE_EDITION` environment variable.
 * This loader is registered in META-INF/services/io.micronaut.context.env.PropertySourceLoader
 * so that it is automatically picked up by Micronaut.
 */
class EditionPropertySourceLoader(
  private val airbyteEdition: String? = System.getenv(AIRBYTE_EDITION_ENV_VAR),
) : YamlPropertySourceLoader() {
  companion object {
    const val AIRBYTE_EDITION_ENV_VAR = "AIRBYTE_EDITION"
  }

  override fun getOrder(): Int = DEFAULT_POSITION + 10

  override fun load(
    name: String?,
    resourceLoader: ResourceLoader?,
  ): Optional<PropertySource> {
    if (airbyteEdition.isNullOrEmpty()) {
      return Optional.empty()
    }

    val fileName = "application-edition-" + airbyteEdition.lowercase()
    return super.load(fileName, resourceLoader)
  }
}
