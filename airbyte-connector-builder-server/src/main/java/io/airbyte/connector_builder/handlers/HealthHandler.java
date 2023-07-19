/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.handlers;

import io.airbyte.connector_builder.api.model.generated.HealthCheckRead;
import jakarta.inject.Singleton;

/**
 * The Health handler gets the CDK version from a CdkVersionProvider and returns a HealthCheckRead
 * indicating that the server is up and identifying the version of the CDK used by the server.
 */
@Singleton
public class HealthHandler {

  private final CachedCdkVersionProviderDecorator cdkVersionProvider;

  public HealthHandler(final CachedCdkVersionProviderDecorator cdkVersionProvider) {
    this.cdkVersionProvider = cdkVersionProvider;
  }

  /**
   * Get the server status and the version of the CDK used.
   */
  public HealthCheckRead getHealthCheck() {
    try {
      final String cdkVersion = cdkVersionProvider.getCdkVersion();
      return new HealthCheckRead().available(true).cdkVersion(cdkVersion);
    } catch (final Exception e) {
      return new HealthCheckRead().available(false);
    }
  }

}
