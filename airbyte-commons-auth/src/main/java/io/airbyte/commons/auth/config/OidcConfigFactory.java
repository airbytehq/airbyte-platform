/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * This factory class is used to create an {@link OidcConfig} bean. It is only necessary because we
 * are transitioning from using `airbyte.auth.identity-providers` to `airbyte.auth.oidc`. This
 * factory creates conditional beans based on whether `airbyte.auth.oidc` is defined in
 * `application.yml`, to allow backwards-compatibility with `airbyte.auth.identity-providers` in
 * `airbyte.yml` for now.
 */
@Factory
@Slf4j
public class OidcConfigFactory {

  /**
   * This bean is used when `airbyte.auth.identity-provider.type` is set to `oidc` in
   * `application.yml`. This is the preferred way to configure OIDC, so this bean will take precedence
   * over the other bean.
   */
  @Singleton
  @Requires(property = "airbyte.auth.identity-provider.type",
            value = "oidc")
  public OidcConfig createOidcConfig(final AuthOidcConfiguration authOidcConfiguration) {
    return new OidcConfig(
        authOidcConfiguration.domain(),
        authOidcConfiguration.appName(),
        authOidcConfiguration.clientId(),
        authOidcConfiguration.clientSecret());
  }

  /**
   * This bean is used for backwards-compatibility with `airbyte.auth.identity-providers` in
   * `airbyte.yml`. Eventually, we will remove support for `airbyte.auth.identity-providers` and only
   * use `airbyte.auth.identity-provider`.
   */
  @Singleton
  @Requires(missingProperty = "airbyte.auth.identity-provider")
  @Requires(property = "airbyte.auth.identity-providers")
  public OidcConfig createOidcConfigFromIdentityProviderConfigurations(final List<IdentityProviderConfiguration> identityProviderConfigurations) {
    // throw an error if there are multiple IDPs configured. We're moving away from supporting a list of
    // IDPs, but for backwards-compatibility, we still support a list of IDPs in `airbyte.yml` as long
    // as it contains only one entry.
    if (identityProviderConfigurations.size() > 1) {
      log.error("Only one identity provider is supported. Found {} identity providers.", identityProviderConfigurations.size());
      throw new RuntimeException("Only one identity provider is supported.");
    }

    log.warn("DEPRECATION WARNING: Using `auth.identity-providers` is deprecated. Please use `auth.oidc` in your airbyte.yaml file instead.");
    return identityProviderConfigurations.getFirst().toOidcConfig();
  }

}
