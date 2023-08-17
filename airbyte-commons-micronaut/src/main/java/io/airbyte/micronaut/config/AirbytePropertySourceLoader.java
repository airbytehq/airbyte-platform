/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.config;

import io.micronaut.context.env.ActiveEnvironment;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.PropertySourceLoader;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.io.ResourceLoader;
import io.micronaut.core.io.file.DefaultFileSystemResourceLoader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Custom Micronaut {@link PropertySourceLoader} that loads the airbyte.yml file and prefixes all
 * properties within with "airbyte.". Micronaut registers this property loader via the
 * resources/META-INF/services/io.micronaut.context.env.PropertySourceLoader file.
 */
@Slf4j
public class AirbytePropertySourceLoader implements PropertySourceLoader {

  static final String AIRBYTE_YML_PATH = "/app/airbyte.yml";
  static final String AIRBYTE_KEY = "airbyte";

  final YamlPropertySourceLoader yamlLoader = new YamlPropertySourceLoader();

  private Optional<InputStream> getAirbyteInputStream() {
    final ResourceLoader resourceLoader = new DefaultFileSystemResourceLoader();
    return resourceLoader.getResourceAsStream(AIRBYTE_YML_PATH);
  }

  @Override
  public Optional<PropertySource> load(final String resourceName, final ResourceLoader resourceLoader) {

    // load the airbyte.yml file as an InputStream, if it exists
    final Optional<InputStream> airbyteConfigFile = getAirbyteInputStream();

    if (airbyteConfigFile.isEmpty()) {
      return Optional.empty();
    }

    try (final InputStream stream = airbyteConfigFile.get()) {
      final var yaml = read(resourceName, stream);

      // Prefix all configuration from that file with airbyte.
      final var prefixedProps = yaml.entrySet().stream().collect(Collectors.toMap(entry -> AIRBYTE_KEY + "." + entry.getKey(), Map.Entry::getValue));
      log.info("Loading properties as: {}", prefixedProps);
      return Optional.of(PropertySource.of(prefixedProps));
    } catch (final IOException e) {
      throw new ConfigurationException("Could not load airbyte.yml configuration file.", e);
    }
  }

  @Override
  public Optional<PropertySource> loadEnv(final String resourceName, final ResourceLoader resourceLoader, final ActiveEnvironment activeEnvironment) {
    return load(resourceName, resourceLoader);
  }

  @Override
  public Map<String, Object> read(final String name, final InputStream input) throws IOException {
    return yamlLoader.read(name, input);
  }

}
