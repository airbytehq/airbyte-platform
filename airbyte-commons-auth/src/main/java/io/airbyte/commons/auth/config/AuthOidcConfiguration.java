/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("airbyte.auth.identity-provider.oidc")
public record AuthOidcConfiguration(
                                    String domain,
                                    String appName,
                                    String clientId,
                                    String clientSecret) {}
