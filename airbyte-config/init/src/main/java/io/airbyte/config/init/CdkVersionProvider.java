/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import com.google.common.io.Resources;
import jakarta.inject.Singleton;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * The provider returns the target cdk version.
 */
@Singleton
public class CdkVersionProvider {

  /**
   * Return the CDK version from the resource bundle. Throws if not available.
   *
   * @return cdk version as string
   */
  public String getCdkVersion() {
    try {
      final URL url = Resources.getResource("CDK_VERSION");

      final String cdkVersion = Resources.toString(url, StandardCharsets.UTF_8).strip();

      return cdkVersion;
    } catch (final Exception e) {
      throw new RuntimeException("Failed to fetch local CDK version", e);
    }
  }

}
