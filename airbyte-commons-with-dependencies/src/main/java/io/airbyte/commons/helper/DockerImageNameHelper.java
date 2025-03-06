/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.helper;

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

  private static final int NAME_PARTS_INDEX = 0;
  private static final int VERSION_PART_INDEX = 1;

  private static final Logger LOGGER = LoggerFactory.getLogger(DockerImageNameHelper.class);

  /**
   * Docker image names are by convention separated by slashes. The last portion is the image's name.
   * This is followed by a colon and a version number. e.g. airbyte/scheduler:v1 or
   * gcr.io/my-project/image-name:v2. Registry name may also include port number, e.g.
   * registry.internal:1234/my-project/image-name:v2
   *
   * @param fullImagePath the image name with repository and version ex
   *        gcr.io/my-project/image-name:v2
   * @return the image name without the repo and version, ex. image-name
   */
  public static String extractShortImageName(final String fullImagePath) {
    final var noVersion = extractImageNameWithoutVersion(fullImagePath);

    final var nameParts = noVersion.split(DOCKER_DELIMITER);
    return nameParts[nameParts.length - 1];
  }

  /**
   * Extracts the image name without the version tag.
   *
   * @param fullImagePath the docker image name
   * @return anything before last ":"
   */
  public static String extractImageNameWithoutVersion(final String fullImagePath) {
    return extractPartFromFullPath(fullImagePath, NAME_PARTS_INDEX);
  }

  /**
   * Extracts the image version label as a string.
   *
   * @param fullImagePath the docker image name
   * @return anything after last ":"
   */
  public static String extractImageVersionString(final String fullImagePath) {
    return extractPartFromFullPath(fullImagePath, VERSION_PART_INDEX);
  }

  /**
   * Extracts the version label and attempts to convert it to a {@link SemanticVersion} object.
   *
   * @param fullImagePath the full docker image name
   * @return an Optional SemanticVersion
   */
  public static Optional<Version> extractImageVersion(final String fullImagePath) {
    final var versionString = Optional.ofNullable(extractImageVersionString(fullImagePath));
    if (versionString.isPresent()) {
      try {
        return Optional.of(new Version(versionString.get()));
      } catch (final Exception e) {
        LOGGER.info("Could not create semantic version from version {}, message: {}", versionString.get(), e.getMessage());
      }
    }
    return Optional.empty();
  }

  private static String extractPartFromFullPath(final String fullImagePath, final int partIndex) {
    final int delimeterIndex = fullImagePath.lastIndexOf(VERSION_DELIMITER);
    if (partIndex == NAME_PARTS_INDEX) {
      return delimeterIndex >= 0 ? fullImagePath.substring(0, delimeterIndex) : fullImagePath;
    } else if (partIndex == VERSION_PART_INDEX) {
      return delimeterIndex >= 0 ? fullImagePath.substring(delimeterIndex + 1) : null;
    } else {
      LOGGER.warn("Invalid part index: {}", partIndex);
      return null;
    }
  }

}
