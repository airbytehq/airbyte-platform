/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.config;

import io.airbyte.commons.envvar.EnvVar;
import io.airbyte.connector_builder.command_runner.SynchronousCdkCommandRunner;
import io.airbyte.connector_builder.command_runner.SynchronousPythonCdkCommandRunner;
import io.airbyte.connector_builder.exceptions.ConnectorBuilderException;
import io.airbyte.connector_builder.file_writer.AirbyteFileWriterImpl;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Defines the instantiation of handler classes.
 */
@Factory
public class ApplicationBeanFactory {

  private String getPython() {
    final var cdkPython = EnvVar.CDK_PYTHON.fetch();
    if (cdkPython == null) {
      throw new ConnectorBuilderException("Missing `CDK_PYTHON` env var.");
    }
    return cdkPython;
  }

  private String getCdkEntrypoint() {
    final var cdkEntrypoint = EnvVar.CDK_ENTRYPOINT.fetch();
    if (cdkEntrypoint == null) {
      throw new ConnectorBuilderException("Missing `CDK_ENTRYPOINT` env var.");
    }
    return cdkEntrypoint;
  }

  /**
   * Defines the instantiation of the SynchronousPythonCdkCommandRunner.
   */
  @Singleton
  public SynchronousCdkCommandRunner synchronousPythonCdkCommandRunner() {
    return new SynchronousPythonCdkCommandRunner(
        new AirbyteFileWriterImpl(),
        // This should eventually be constructed via DI.
        VersionedAirbyteStreamFactory.noMigrationVersionedAirbyteStreamFactory(),
        this.getPython(),
        this.getCdkEntrypoint(),
        this.getPythonPath());
  }

  private String getPythonPath() {
    String pathToConnectors = getPathToConnectors();
    List<String> subdirectories = listSubdirectories(pathToConnectors);
    return createPythonPathFromListOfPaths(pathToConnectors, subdirectories);
  }

  private String getPathToConnectors() {
    return "/connectors";
  }

  private static List<String> listSubdirectories(String path) {
    File file = new File(path);
    String[] directories = file.list((current, name) -> new File(current, name).isDirectory());
    return Optional.ofNullable(directories).stream()
        .flatMap(Arrays::stream)
        .collect(Collectors.toList());
  }

  static String createPythonPathFromListOfPaths(String path, List<String> subdirectories) {
    /*
     * Creates a `:`-separated path of all connector directories. The connector directories that contain
     * a python module can then be imported.
     */
    return subdirectories.stream()
        .map(subdirectory -> path + "/" + subdirectory)
        .collect(Collectors.joining(":"));
  }

}
