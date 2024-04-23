/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config;

public record OidcConfig(String domain, String appName, String clientId, String clientSecret) {}
