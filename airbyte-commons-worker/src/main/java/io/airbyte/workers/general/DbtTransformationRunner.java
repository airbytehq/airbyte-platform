/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static io.airbyte.workers.process.Metadata.CUSTOM_STEP;
import static io.airbyte.workers.process.Metadata.JOB_TYPE_KEY;
import static io.airbyte.workers.process.Metadata.SYNC_JOB;
import static io.airbyte.workers.process.Metadata.SYNC_STEP_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.commons.helper.DockerImageNameHelper;
import io.airbyte.commons.io.LineGobbler;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.LoggingHelper;
import io.airbyte.commons.logging.LoggingHelper.Color;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.logging.MdcScope.Builder;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.OperatorDbt;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.ProcessFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.tools.ant.types.Commandline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DbtTransformationRunner. A large portion of this code is taken from the legacy DBT-based
 * Normalization Runner. Historically, this was done to reuse DBT set up. However, with the
 * deprecation of the DBT-based runner, and the unknown future of Custom Transformation support in
 * OSS, we are not investing in refactoring this code for now.
 */
public class DbtTransformationRunner implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DbtTransformationRunner.class);
  private static final String DBT_ENTRYPOINT_SH = "entrypoint.sh";
  private static final MdcScope.Builder CONTAINER_LOG_MDC_BUILDER = new Builder()
      .setLogPrefix(LoggingHelper.CUSTOM_TRANSFORMATION_LOGGER_PREFIX)
      .setPrefixColor(Color.PURPLE_BACKGROUND);

  private final ProcessFactory processFactory;
  private final String destinationImage;
  private Process process = null;

  public DbtTransformationRunner(final ProcessFactory processFactory, final String destinationImage) {
    this.processFactory = processFactory;
    this.destinationImage = destinationImage;
  }

  /**
   * The docker image used by the DbtTransformationRunner is provided by the User, so we can't ensure
   * to have the right python, dbt, dependencies etc software installed to successfully run our
   * transform-config scripts (to translate Airbyte Catalogs into Dbt profiles file). Thus, we depend
   * on a pre-build prep image to configure the dbt project with the appropriate destination settings
   * and pull the custom git repository into the workspace.
   * <p>
   * Once the workspace folder/files is setup to run, we invoke the custom transformation command as
   * provided by the user to execute whatever extra transformation has been implemented.
   */
  public boolean run(final String jobId,
                     final int attempt,
                     final UUID connectionId,
                     final UUID workspaceId,
                     final Path jobRoot,
                     final JsonNode config,
                     final ResourceRequirements resourceRequirements,
                     final OperatorDbt dbtConfig)
      throws Exception {
    if (!configureDbt(jobId, attempt, connectionId, workspaceId, jobRoot, config, resourceRequirements, dbtConfig)) {
      return false;
    }
    return transform(jobId, attempt, jobRoot, resourceRequirements, dbtConfig);
  }

  /**
   * Transform data (i.e. run normalization).
   *
   * @param jobId job id
   * @param attempt attempt number
   * @param jobRoot job root
   * @param resourceRequirements resource requirements
   * @param dbtConfig dbt config
   * @return true, if succeeded. otherwise, false.
   * @throws Exception while executing
   */
  public boolean transform(final String jobId,
                           final int attempt,
                           final Path jobRoot,
                           final ResourceRequirements resourceRequirements,
                           final OperatorDbt dbtConfig)
      throws Exception {
    try {
      final Map<String, String> files = ImmutableMap.of(
          DBT_ENTRYPOINT_SH, MoreResources.readResource("dbt_transformation_entrypoint.sh"),
          "sshtunneling.sh", MoreResources.readResource("sshtunneling.sh"));
      final List<String> dbtArguments = new ArrayList<>();
      dbtArguments.add(DBT_ENTRYPOINT_SH);
      if (Strings.isNullOrEmpty(dbtConfig.getDbtArguments())) {
        throw new WorkerException("Dbt Arguments are required");
      }
      Collections.addAll(dbtArguments, Commandline.translateCommandline(dbtConfig.getDbtArguments()));
      process =
          processFactory.create(
              ResourceType.DEFAULT,
              CUSTOM_STEP,
              jobId,
              attempt,
              null, // TODO: Provide connectionId
              null, // TODO: Provide workspaceId
              jobRoot,
              dbtConfig.getDockerImage(),
              false,
              false,
              files,
              "/bin/bash",
              // We should use the AirbyteIntegrationLauncher instead
              AirbyteIntegrationLauncher.buildGenericConnectorResourceRequirements(resourceRequirements),
              null,
              Map.of(JOB_TYPE_KEY, SYNC_JOB, SYNC_STEP_KEY, CUSTOM_STEP),
              Collections.emptyMap(),
              Collections.emptyMap(),
              Collections.emptyMap(), dbtArguments.toArray(new String[0]));
      LineGobbler.gobble(process.getInputStream(), LOGGER::info, CONTAINER_LOG_MDC_BUILDER);
      LineGobbler.gobble(process.getErrorStream(), LOGGER::error, CONTAINER_LOG_MDC_BUILDER);

      WorkerUtils.wait(process);

      return process.exitValue() == 0;
    } catch (final Exception e) {
      // make sure we kill the process on failure to avoid zombies.
      if (process != null) {
        WorkerUtils.cancelProcess(process);
      }
      throw e;
    }
  }

  @Override
  public void close() throws Exception {

    if (process == null) {
      return;
    }

    LOGGER.debug("Closing dbt transformation process");
    WorkerUtils.gentleClose(process, 1, TimeUnit.MINUTES);
    if (process.isAlive() || process.exitValue() != 0) {
      throw new WorkerException("Dbt transformation process wasn't successful");
    }
  }

  /*
   * FROM HERE ON, CODE IS ADAPTED FROM THE LEGACY DBT-BASED NORMALIZATION RUNNER.
   */

  /**
   * Prepare a configured folder to run dbt commands from (similar to what is required by
   * normalization models) However, this does not run the normalization file generation process or dbt
   * at all. This is pulling files from a distant git repository instead of the dbt-project-template.
   *
   * @return true if configuration succeeded. otherwise false.
   * @throws Exception - any exception thrown from configuration will be handled gracefully by the
   *         caller.
   */
  public boolean configureDbt(final String jobId,
                              final int attempt,
                              final UUID connectionId,
                              final UUID workspaceId,
                              final Path jobRoot,
                              final JsonNode config,
                              final ResourceRequirements resourceRequirements,
                              final OperatorDbt dbtConfig)
      throws Exception {
    final Map<String, String> files = ImmutableMap.of(
        WorkerConstants.DESTINATION_CONFIG_JSON_FILENAME, Jsons.serialize(config));
    final String gitRepoUrl = dbtConfig.getGitRepoUrl();
    final String type = getAirbyteDestinationName(destinationImage);
    if (Strings.isNullOrEmpty(gitRepoUrl)) {
      throw new WorkerException("Git Repo Url is required");
    }
    final String gitRepoBranch = dbtConfig.getGitRepoBranch();
    if (Strings.isNullOrEmpty(gitRepoBranch)) {
      return runConfigureProcess(jobId, attempt, connectionId, workspaceId, jobRoot, files, resourceRequirements, "configure-dbt",
          "--integration-type", type,
          "--config", WorkerConstants.DESTINATION_CONFIG_JSON_FILENAME,
          "--git-repo", gitRepoUrl);
    } else {
      return runConfigureProcess(jobId, attempt, connectionId, workspaceId, jobRoot, files, resourceRequirements, "configure-dbt",
          "--integration-type", type,
          "--config", WorkerConstants.DESTINATION_CONFIG_JSON_FILENAME,
          "--git-repo", gitRepoUrl,
          "--git-branch", gitRepoBranch);
    }
  }

  /**
   * Extract the destination name from the docker image name. This is an artifact of the old
   * dbt-based-normalization set up process that needs to know what destination it is processing in
   * order to correctly parse the destination config, connect to the destination, and run the
   * transformation.
   *
   * @param image name with attached version prefixed with destination- e.g.
   *        airbyte/destination-snowflake:0.1.0.
   */
  @VisibleForTesting
  protected static String getAirbyteDestinationName(String image) {
    return DockerImageNameHelper.extractImageNameWithoutVersion(image).split("/")[1].split("-")[1];
  }

  @VisibleForTesting
  protected boolean runConfigureProcess(final String jobId,
                                        final int attempt,
                                        final UUID connectionId,
                                        final UUID workspaceId,
                                        final Path jobRoot,
                                        final Map<String, String> files,
                                        final ResourceRequirements resourceRequirements,
                                        final String... args)
      throws Exception {
    try {
      process = processFactory.create(
          ResourceType.DEFAULT,
          CUSTOM_STEP,
          jobId,
          attempt,
          connectionId,
          workspaceId,
          jobRoot,
          "airbyte/custom-transformation-prep:1.0",
          // custom connector does not use normalization
          false,
          false, files,
          null,
          AirbyteIntegrationLauncher.buildGenericConnectorResourceRequirements(resourceRequirements),
          null,
          Map.of(JOB_TYPE_KEY, SYNC_JOB, SYNC_STEP_KEY, CUSTOM_STEP),
          Collections.emptyMap(),
          Collections.emptyMap(),
          Collections.emptyMap(), args);
      LineGobbler.gobble(process.getInputStream(), LOGGER::info, CONTAINER_LOG_MDC_BUILDER);
      LineGobbler.gobble(process.getErrorStream(), LOGGER::error, CONTAINER_LOG_MDC_BUILDER);

      WorkerUtils.wait(process);
      return process.exitValue() == 0;
    } catch (final Exception e) {
      // make sure we kill the process on failure to avoid zombies.
      if (process != null) {
        WorkerUtils.cancelProcess(process);
      }
      throw e;
    }
  }

}
