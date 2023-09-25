/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.config;

import io.airbyte.config.EnvConfigs;
import io.airbyte.connector_builder.command_runner.SynchronousCdkCommandRunner;
import io.airbyte.connector_builder.command_runner.SynchronousPythonCdkCommandRunner;
import io.airbyte.connector_builder.exceptions.ConnectorBuilderException;
import io.airbyte.connector_builder.file_writer.AirbyteFileWriterImpl;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

/**
 * Defines the instantiation of handler classes.
 */
@Factory
public class ApplicationBeanFactory {

  private String getPython() {
    final EnvConfigs configs = new EnvConfigs();
    if (configs.getCdkPython() == null) {
      throw new ConnectorBuilderException("Missing `CDK_PYTHON` env var.");
    }
    return configs.getCdkPython();
  }

  private String getCdkEntrypoint() {
    final EnvConfigs configs = new EnvConfigs();
    if (configs.getCdkEntrypoint() == null) {
      throw new ConnectorBuilderException("Missing `CDK_ENTRYPOINT` env var.");
    }
    return configs.getCdkEntrypoint();
  }

  /**
   * Defines the instantiation of the SynchronousPythonCdkCommandRunner.
   */
  @Singleton
  public SynchronousCdkCommandRunner synchronousPythonCdkCommandRunner() {
    return new SynchronousPythonCdkCommandRunner(
        new AirbyteFileWriterImpl(),
        // This should eventually be constructed via DI.
        VersionedAirbyteStreamFactory.noMigrationVersionedAirbyteStreamFactory(false),
        this.getPython(),
        this.getCdkEntrypoint());
  }

}
