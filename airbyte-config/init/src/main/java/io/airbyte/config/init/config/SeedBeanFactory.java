/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init.config;

import io.airbyte.config.specs.DefinitionsProvider;
import io.airbyte.config.specs.LocalDefinitionsProvider;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.LoggerFactory;

/**
 * Micronaut bean factory for singletons related to seeding the database.
 */
@Factory
public class SeedBeanFactory {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SeedBeanFactory.class);

  private static final String LOCAL_SEED_PROVIDER = "LOCAL";
  private static final String REMOTE_SEED_PROVIDER = "REMOTE";

  /**
   * Creates a singleton {@link DefinitionsProvider} based on the seed provider specified in the
   * config, to be used when loading definitions into the DB in cron or bootloader.
   */
  @Singleton
  @Named("seedDefinitionsProvider")
  public DefinitionsProvider seedDefinitionsProvider(@Value("${airbyte.connector-registry.seed-provider}") final String seedProvider,
                                                     final RemoteDefinitionsProvider remoteDefinitionsProvider) {
    if (StringUtils.isEmpty(seedProvider) || LOCAL_SEED_PROVIDER.equalsIgnoreCase(seedProvider)) {
      LOGGER.info("Using local definitions provider for seeding");
      return new LocalDefinitionsProvider();
    } else if (REMOTE_SEED_PROVIDER.equalsIgnoreCase(seedProvider)) {
      LOGGER.info("Using remote definitions provider for seeding");
      return remoteDefinitionsProvider;
    }

    throw new IllegalArgumentException("Invalid seed provider: " + seedProvider);
  }

}
