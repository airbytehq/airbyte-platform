/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.PlainJWT
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER
import io.airbyte.commons.server.support.CurrentUserService
import io.micronaut.http.HttpRequest
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class AuthenticationFactoryTest {
  @Test
  fun `client reference headers do not affect authentication roles`() {
    val scopeResolver = mockk<AuthenticationHeaderResolver>(relaxed = true)
    val permissions = mockk<PermissionHandler>()
    every { permissions.getPermissionsByAuthUserId("caller") } returns emptyList()
    val factory = AuthenticationFactory(RoleResolver(scopeResolver, mockk<CurrentUserService>(), null, permissions))
    val token = PlainJWT(JWTClaimsSet.Builder().subject("caller").build())
    val request = HttpRequest.GET<Any>("/api/v1/workspaces/get").header(WORKSPACE_ID_HEADER, UUID.randomUUID().toString())

    lateinit var authentication: Authentication
    ServerRequestContext.with(request) { authentication = factory.createAuthentication(token).orElseThrow() }

    assertEquals("caller", authentication.name)
    verify { scopeResolver.resolveScope(emptyMap(), any(), any()) }
  }
}
