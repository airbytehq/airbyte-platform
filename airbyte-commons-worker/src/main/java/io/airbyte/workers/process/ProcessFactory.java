/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import io.airbyte.commons.helper.DockerImageNameHelper;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.AllowedHosts;
import io.airbyte.workers.exception.WorkerException;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Factory for creating a process.
 */
public interface ProcessFactory {

  String VERSION_DELIMITER = ":";
  String DOCKER_DELIMITER = "/";
  Pattern ALPHABETIC = Pattern.compile("[a-zA-Z]+");

  /**
   * Creates a ProcessBuilder to run a program in a new Process.
   *
   * @param resourceType type of the resource. This is used to pull the base configuration from the
   *        WorkerConfigsProvider.
   * @param jobType type of job to add to name for easier operational processes.
   * @param jobId job Id
   * @param attempt attempt Id
   * @param jobPath Workspace directory to run the process from.
   * @param imageName Docker image name to start the process from.
   * @param usesIsolatedPool whether to use isolated pool to run the jobs.
   * @param files File name to contents map that will be written into the working dir of the process
   *        prior to execution.
   * @param entrypoint If not null, the default entrypoint program of the docker image can be changed
   *        by this argument.
   * @param connectorResourceRequirements CPU and RAM to assign to the created process.
   * @param labels Labels to assign to the created Kube pod, if any. Ignore for docker.
   * @param jobMetadata Job metadata that will be passed to the created process as environment
   *        variables.
   * @param additionalEnvironmentVariables Additional Environment Variables to pass into the process
   * @param args Arguments to pass to the docker image being run in the new process.
   * @return ProcessBuilder object to run the process.
   * @throws WorkerException exception thrown from worker
   */
  Process create(final ResourceType resourceType,
                 final String jobType,
                 final String jobId,
                 final int attempt,
                 final UUID connectionId,
                 final UUID workspaceId,
                 final Path jobPath,
                 final String imageName,
                 final boolean usesIsolatedPool,
                 final boolean usesStdin,
                 final Map<String, String> files,
                 final String entrypoint,
                 final ConnectorResourceRequirements connectorResourceRequirements,
                 final AllowedHosts allowedHosts,
                 final Map<String, String> labels,
                 final Map<String, String> jobMetadata,
                 final Map<Integer, Integer> portMapping,
                 Map<String, String> additionalEnvironmentVariables,
                 final String... args)
      throws WorkerException;

  /**
   * Docker image names are by convention separated by slashes. The last portion is the image's name.
   * This is followed by a colon and a version number. e.g. airbyte/scheduler:v1 or
   * gcr.io/my-project/image-name:v2.
   *
   * With these two facts, attempt to construct a unique process name with the image name present that
   * can be used by the factories implementing this interface for easier operations.
   */
  static String createProcessName(final String fullImagePath, final String jobType, final String jobId, final int attempt, final int lenLimit) {

    var imageName = DockerImageNameHelper.extractShortImageName(fullImagePath);
    final var randSuffix = RandomStringUtils.randomAlphabetic(5).toLowerCase();
    final String suffix = jobType + "-" + jobId + "-" + attempt + "-" + randSuffix;

    var processName = imageName + "-" + suffix;
    if (processName.length() > lenLimit) {
      final var extra = processName.length() - lenLimit;
      imageName = imageName.substring(extra);
      processName = imageName + "-" + suffix;
    }

    // Kubernetes pod names must start with an alphabetic character while Docker names accept
    // alphanumeric.
    // Use the stricter convention for simplicity.
    final Matcher m = ALPHABETIC.matcher(processName);
    // Since we add sync-UUID as a suffix a couple of lines up, there will always be a substring
    // starting with an alphabetic character.
    // If the image name is a no-op, this function should always return `sync-UUID` at the minimum.
    m.find();
    return processName.substring(m.start());
  }

  /**
   * Extract the shortname of the image from the full path.
   */
  static String getShortImageName(final String fullImagePath) {
    if (fullImagePath == null || fullImagePath.length() < 1) {
      return null;
    }
    return DockerImageNameHelper.extractShortImageName(fullImagePath);
  }

  /**
   * Docker image names are by convention separated by slashes. The last portion is the image's name.
   * This is followed by a colon and a version number. e.g. airbyte/scheduler:v1 or
   * gcr.io/my-project/image-name:v2.
   *
   * Get the image version by returning the substring following a colon.
   */
  static String getImageVersion(final String fullImagePath) {
    if (fullImagePath == null || fullImagePath.length() < 1) {
      return null;
    }
    int colonIndex = fullImagePath.indexOf(":");
    if (colonIndex != -1 && fullImagePath.length() > colonIndex + 1) {
      return fullImagePath.substring(colonIndex + 1);
    }
    return null;
  }

}
