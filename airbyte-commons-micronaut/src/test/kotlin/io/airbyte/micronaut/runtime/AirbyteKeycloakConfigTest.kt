/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteKeycloakConfigDefaultTest {
  @Inject
  private lateinit var airbyteKeycloakConfig: AirbyteKeycloakConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_KEYCLOAK_AIRBYTE_REALM, airbyteKeycloakConfig.airbyteRealm)
    assertEquals(DEFAULT_KEYCLOAK_BASE_PATH, airbyteKeycloakConfig.basePath)
    assertEquals(DEFAULT_KEYCLOAK_CLIENT_ID, airbyteKeycloakConfig.clientId)
    assertEquals(DEFAULT_KEYCLOAK_CLIENT_REALM, airbyteKeycloakConfig.clientRealm)
    assertEquals("", airbyteKeycloakConfig.host)
    assertEquals(DEFAULT_KEYCLOAK_INTERNAL_REALM, airbyteKeycloakConfig.internalRealm)
    assertEquals(DEFAULT_KEYCLOAK_PASSWORD, airbyteKeycloakConfig.password)
    assertEquals(DEFAULT_KEYCLOAK_PROTOCOL, airbyteKeycloakConfig.protocol)
    assertEquals(DEFAULT_KEYCLOAK_REALM, airbyteKeycloakConfig.realm)
    assertEquals(false, airbyteKeycloakConfig.resetRealm)
    assertEquals(DEFAULT_KEYCLOAK_USERNAME, airbyteKeycloakConfig.username)
    assertEquals(DEFAULT_KEYCLOAK_WEB_CLIENT_ID, airbyteKeycloakConfig.webClientId)
  }
}

@MicronautTest(propertySources = ["classpath:application-keycloak.yml"])
internal class AirbyteKeycloakConfigStiggTest {
  @Inject
  private lateinit var airbyteKeycloakConfig: AirbyteKeycloakConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-airbyte-realm", airbyteKeycloakConfig.airbyteRealm)
    assertEquals("test-base-path", airbyteKeycloakConfig.basePath)
    assertEquals("test-client-id", airbyteKeycloakConfig.clientId)
    assertEquals("test-client-realm", airbyteKeycloakConfig.clientRealm)
    assertEquals("test-host", airbyteKeycloakConfig.host)
    assertEquals("test-internal-realm", airbyteKeycloakConfig.internalRealm)
    assertEquals("test-password", airbyteKeycloakConfig.password)
    assertEquals("test-protocol", airbyteKeycloakConfig.protocol)
    assertEquals("test-realm", airbyteKeycloakConfig.realm)
    assertEquals(true, airbyteKeycloakConfig.resetRealm)
    assertEquals("test-username", airbyteKeycloakConfig.username)
    assertEquals("test-web-client-id", airbyteKeycloakConfig.webClientId)
  }
}
