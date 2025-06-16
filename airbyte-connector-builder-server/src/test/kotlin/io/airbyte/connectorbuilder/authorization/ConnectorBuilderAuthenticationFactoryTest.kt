/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.authorization

import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.PlainJWT
import io.airbyte.commons.auth.roles.AuthRole
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ConnectorBuilderAuthenticationFactoryTest {
  private lateinit var factory: ConnectorBuilderAuthenticationFactory

  @BeforeEach
  fun setup() {
    factory = ConnectorBuilderAuthenticationFactory()
  }

  @Test
  fun `test resolveRoles with null authUserId`() {
    val auth = factory.createAuthentication(PlainJWT(JWTClaimsSet.Builder().build()))
    assertEquals(true, auth.isEmpty)
  }

  @Test
  fun `test resolveRoles with blank authUserId`() {
    val auth = factory.createAuthentication(PlainJWT(JWTClaimsSet.Builder().subject("  ").build()))
    assertEquals(true, auth.isEmpty)
  }

  @Test
  fun `test resolveRoles with valid authUserId`() {
    val roles =
      factory
        .createAuthentication(PlainJWT(JWTClaimsSet.Builder().subject("test").build()))
        .get()
        .roles
        .toSet()
    assertEquals(setOf(AuthRole.AUTHENTICATED_USER.name), roles)
  }
}
