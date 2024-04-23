/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config;

import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MicronautTest
public class IdentityProviderConfigurationTest {

  @Inject
  IdentityProviderConfiguration identityProviderConfiguration;

  @Test
  @Property(name = "airbyte.auth.identity-providers[0].type",
            value = "oidc")
  @Property(name = "airbyte.auth.identity-providers[0].domain",
            value = "testdomain")
  @Property(name = "airbyte.auth.identity-providers[0].appName",
            value = "testApp")
  @Property(name = "airbyte.auth.identity-providers[0].clientId",
            value = "testClientId")
  @Property(name = "airbyte.auth.identity-providers[0].clientSecret",
            value = "testClientSecret")
  void testToAuthOidcConfiguration() {
    final OidcConfig result = identityProviderConfiguration.toOidcConfig();
    Assertions.assertEquals("testdomain", result.domain());
    Assertions.assertEquals("testApp", result.appName());
    Assertions.assertEquals("testClientId", result.clientId());
    Assertions.assertEquals("testClientSecret", result.clientSecret());
  }

}
