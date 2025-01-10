/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader;

import io.airbyte.config.init.ApplyDefinitionsHelper;
import io.airbyte.config.init.DeclarativeSourceUpdater;
import io.airbyte.config.init.PostLoadExecutor;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the tasks that should be executed after a successful bootstrapping of
 * the Airbyte environment.
 * <p>
 * <p>
 * This implementation performs the following tasks:
 * <ul>
 * <li>Applies the latest definitions from the provider to the repository</li>
 * <li>If enables, migrates secrets</li>
 * </ul>
 */
@Singleton
public class DefaultPostLoadExecutor implements PostLoadExecutor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ApplyDefinitionsHelper applyDefinitionsHelper;
  private final DeclarativeSourceUpdater declarativeSourceUpdater;
  private final Optional<AuthKubernetesSecretInitializer> authSecretInitializer;

  public DefaultPostLoadExecutor(final ApplyDefinitionsHelper applyDefinitionsHelper,
                                 @Named("localDeclarativeSourceUpdater") final DeclarativeSourceUpdater declarativeSourceUpdater,
                                 final Optional<AuthKubernetesSecretInitializer> authSecretInitializer) {
    this.applyDefinitionsHelper = applyDefinitionsHelper;
    this.declarativeSourceUpdater = declarativeSourceUpdater;
    this.authSecretInitializer = authSecretInitializer;
  }

  @Override
  public void execute() throws Exception {
    log.info("Updating connector definitions");
    applyDefinitionsHelper.apply(false, true);
    log.info("Done updating connector definitions");
    declarativeSourceUpdater.apply();
    log.info("Loaded seed data.");

    if (authSecretInitializer.isPresent()) {
      log.info("Initializing auth secrets");
      authSecretInitializer.get().initializeSecrets();
      log.info("Done initializing auth secrets");
    } else {
      log.info("Auth secret initializer not present. Skipping.");
    }
  }

}
