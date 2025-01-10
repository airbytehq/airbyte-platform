/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.pod;

import io.airbyte.commons.helper.DockerImageNameHelper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Left-over utility methods from the various old process-related singletons and factories.
 */
public class PodUtils {

  static Pattern ALPHABETIC = Pattern.compile("[a-zA-Z]+");

  /**
   * Docker image names are by convention separated by slashes. The last portion is the image's name.
   * This is followed by a colon and a version number. e.g. airbyte/scheduler:v1 or
   * gcr.io/my-project/image-name:v2.
   * <p>
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
    if (fullImagePath == null || fullImagePath.isEmpty()) {
      return null;
    }
    return DockerImageNameHelper.extractShortImageName(fullImagePath);
  }

  /**
   * Docker image names are by convention separated by slashes. The last portion is the image's name.
   * This is followed by a colon and a version number. e.g. airbyte/scheduler:v1 or
   * gcr.io/my-project/image-name:v2. Registry name may also include port number, e.g.
   * registry.internal:1234/my-project/image-name:v2
   * <p />
   * Get the image version by returning the substring following a colon.
   */
  static String getImageVersion(final String fullImagePath) {
    if (fullImagePath == null || fullImagePath.isEmpty()) {
      return null;
    }
    // Use the last colon to find the image tag
    int colonIndex = fullImagePath.lastIndexOf(":");
    if (colonIndex != -1 && fullImagePath.length() > colonIndex + 1) {
      return fullImagePath.substring(colonIndex + 1);
    }
    return null;
  }

}
