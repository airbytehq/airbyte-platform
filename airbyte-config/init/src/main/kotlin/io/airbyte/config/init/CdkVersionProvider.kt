/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

import com.google.common.io.Resources
import jakarta.inject.Singleton
import java.nio.charset.StandardCharsets

/**
 * The provider returns the target cdk version.
 */
@Singleton
class CdkVersionProvider {
  val cdkVersion: String
    /**
     * Return the CDK version from the resource bundle. Throws if not available.
     *
     * @return cdk version as string
     */
    get() {
      try {
        val url = Resources.getResource("CDK_VERSION")

        val cdkVersion = Resources.toString(url, StandardCharsets.UTF_8).trim()

        return cdkVersion
      } catch (e: Exception) {
        throw RuntimeException("Failed to fetch local CDK version", e)
      }
    }
}
