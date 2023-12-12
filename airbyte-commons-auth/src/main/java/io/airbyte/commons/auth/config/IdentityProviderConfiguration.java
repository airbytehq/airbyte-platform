/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config;

import io.micronaut.context.annotation.EachProperty;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * This class pulls together all the auth identity provider configuration as defined in airbyte.yml.
 * It can be injected as a single dependency, rather than injecting each individual value
 * separately.
 */
@EachProperty(value = "airbyte.auth.identity-providers",
              list = true)
@Getter
@Slf4j
@ToString
public class IdentityProviderConfiguration {

  public enum ProviderType {
    OKTA
  }

  ProviderType type;
  String domain;
  String appName;
  String clientId;
  String clientSecret;

}
