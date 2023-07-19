/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.handlers;

import io.airbyte.config.init.CdkVersionProvider;
import jakarta.inject.Singleton;

/**
 * Wrapper around the CdkVersionProvider that caches the cdk version.
 */
@Singleton
public class CachedCdkVersionProviderDecorator {

  private final CdkVersionProvider delegate;
  private String cdkVersion;

  public CachedCdkVersionProviderDecorator(final CdkVersionProvider delegate) {
    this.delegate = delegate;
    this.cdkVersion = null;
  }

  /**
   * Return the CDK version from the delegate the first time. After, return cached value
   *
   * @return cdk version as string
   */
  public String getCdkVersion() {
    if (this.cdkVersion == null) {
      this.cdkVersion = this.delegate.getCdkVersion();
    }
    return this.cdkVersion;
  }

}
