/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import io.airbyte.commons.version.Version;
import io.micronaut.core.version.SemanticVersion;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility Methods for working with Airbyte's docker image names and tags.
 */
public class DockerImageNameHelper {

  static final String VERSION_DELIMITER = ":";
  static final String DOCKER_DELIMITER = "/";

  private static final Logger LOGGER = LoggerFactory.getLogger(DockerImageNameHelper.class);

  /**
   * Docker image names are by convention separated by slashes. The last portion is the image's name.
   * This is followed by a colon and a version number. e.g. airbyte/scheduler:v1 or
   * gcr.io/my-project/my-project:v2.
   *
   * @param fullImagePath the image name with repository and version ex
   *        gcr.io/my-project/image-name:v2
   * @return the image name without the repo and version, ex. image-name
   */
  public static String extractShortImageName(final String fullImagePath) {
    final var noVersion = fullImagePath.split(VERSION_DELIMITER)[0];

    final var nameParts = noVersion.split(DOCKER_DELIMITER);
    return nameParts[nameParts.length - 1];
  }

  /**
   * Extracts the image version label as a string.
   *
   * @param fullImagePath the docker image name
   * @return anything after ":"
   */
  public static String extractImageVersionString(final String fullImagePath) {
    final String[] parts = fullImagePath.split(VERSION_DELIMITER);
    return parts.length == 2 ? parts[1] : null;
  }

  /**
   * Extracts the version label and attempts to convert it to a {@link SemanticVersion} object.
   *
   * @param fullImagePath the full docker image name
   * @return an Optional SemanticVersion
   */
  public static Optional<Version> extractImageVersion(final String fullImagePath) {
    var versionString = Optional.ofNullable(extractImageVersionString(fullImagePath));
    if (versionString.isPresent()) {
      try {
        return Optional.of(new Version(versionString.get()));
      } catch (Exception e) {
        LOGGER.info("Could not create semantic version from version {}, message: {}", versionString.get(), e.getMessage());
      }
    }
    return Optional.empty();
  }

}
