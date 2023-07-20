/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * This class pulls together all the auth user configuration as defined in airbyte.yml. It can be
 * injected as a single dependency, rather than injecting each individual value separately.
 */
@ConfigurationProperties("airbyte.initial-user")
@Getter
@Slf4j
public class InitialUserConfiguration {

  String email;
  String firstName;
  String lastName;
  String username;
  String password;

}
