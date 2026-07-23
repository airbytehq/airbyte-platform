/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.DefaultFullHttpRequest
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.UUID

internal class AuthorizationServerHandlerTest {
  private lateinit var handler: AuthorizationServerHandler
  private lateinit var context: ChannelHandlerContext

  @BeforeEach
  fun setup() {
    val extractor = AirbyteHttpRequestFieldExtractor()
    handler = AuthorizationServerHandler(extractor)
    context = mockk(relaxed = true)
    every { context.fireChannelRead(any()) } returns context
  }

  private fun buildRequest(
    uri: String,
    body: String,
  ): DefaultFullHttpRequest {
    val content = Unpooled.copiedBuffer(body, StandardCharsets.UTF_8)
    return DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri, content)
  }

  @Test
  fun testBodyValueSetsHeader() {
    val workspaceId = UUID.randomUUID()
    val body = """{"workspaceId":"$workspaceId"}"""
    val request = buildRequest("/api/v1/connections/list", body)

    handler.channelRead(context, request)

    assertEquals(
      workspaceId.toString(),
      request.headers().get(AuthenticationHttpHeaders.WORKSPACE_ID_HEADER),
    )
  }

  @Test
  fun testClientSuppliedHeaderIsOverwrittenByBodyValue() {
    val attackerWorkspaceId = UUID.randomUUID()
    val legitimateWorkspaceId = UUID.randomUUID()
    val body = """{"workspaceId":"$legitimateWorkspaceId"}"""
    val request = buildRequest("/api/v1/connections/list", body)
    request.headers().add(AuthenticationHttpHeaders.WORKSPACE_ID_HEADER, attackerWorkspaceId.toString())

    handler.channelRead(context, request)

    assertEquals(
      legitimateWorkspaceId.toString(),
      request.headers().get(AuthenticationHttpHeaders.WORKSPACE_ID_HEADER),
    )
  }

  @Test
  fun testClientSuppliedHeaderIsStrippedWhenFieldNotInBody() {
    val attackerOrgId = UUID.randomUUID()
    val body = """{"workspaceId":"${UUID.randomUUID()}"}"""
    val request = buildRequest("/api/v1/connections/list", body)
    request.headers().add(AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER, attackerOrgId.toString())

    handler.channelRead(context, request)

    assertFalse(
      request.headers().contains(AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER),
    )
  }

  @Test
  fun testMultipleSpoofedHeadersAreAllStripped() {
    val body = """{"sourceId":"${UUID.randomUUID()}"}"""
    val request = buildRequest("/api/v1/sources/get", body)
    request.headers().add(AuthenticationHttpHeaders.WORKSPACE_ID_HEADER, UUID.randomUUID().toString())
    request.headers().add(AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER, UUID.randomUUID().toString())
    request.headers().add(AuthenticationHttpHeaders.CONNECTION_ID_HEADER, UUID.randomUUID().toString())

    handler.channelRead(context, request)

    assertFalse(request.headers().contains(AuthenticationHttpHeaders.WORKSPACE_ID_HEADER))
    assertFalse(request.headers().contains(AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER))
    assertFalse(request.headers().contains(AuthenticationHttpHeaders.CONNECTION_ID_HEADER))
  }

  @Test
  fun testNonAuthHeadersArePreserved() {
    val body = """{"workspaceId":"${UUID.randomUUID()}"}"""
    val request = buildRequest("/api/v1/connections/list", body)
    request.headers().add("X-Airbyte-Client-ID", "sonar-production")
    request.headers().add("Authorization", "Bearer some-token")

    handler.channelRead(context, request)

    assertEquals("sonar-production", request.headers().get("X-Airbyte-Client-ID"))
    assertEquals("Bearer some-token", request.headers().get("Authorization"))
  }

  @Test
  fun testSkippedEndpointsStillStripAuthHeaders() {
    val workspaceId = UUID.randomUUID()
    val request = buildRequest("/api/v1/health", "")
    request.headers().add(AuthenticationHttpHeaders.WORKSPACE_ID_HEADER, workspaceId.toString())

    handler.channelRead(context, request)

    assertFalse(
      request.headers().contains(AuthenticationHttpHeaders.WORKSPACE_ID_HEADER),
    )
  }

  @Test
  fun testEmbeddedWidgetEndpointStripsAuthHeaders() {
    val orgId = UUID.randomUUID()
    val request = buildRequest("/api/public/v1/embedded/widget", "")
    request.headers().add(AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER, orgId.toString())
    request.headers().add("Authorization", "Bearer some-token")

    handler.channelRead(context, request)

    assertFalse(request.headers().contains(AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER))
    assertEquals("Bearer some-token", request.headers().get("Authorization"))
  }

  @Test
  fun testSkippedEndpointsPreserveNonAuthHeaders() {
    val request = buildRequest("/api/v1/health", "")
    request.headers().add("X-Airbyte-Client-ID", "sonar-production")
    request.headers().add("Authorization", "Bearer some-token")

    handler.channelRead(context, request)

    assertEquals("sonar-production", request.headers().get("X-Airbyte-Client-ID"))
    assertEquals("Bearer some-token", request.headers().get("Authorization"))
  }

  @Test
  fun `SCIM payloads skip body extraction while spoofed internal headers are stripped`() {
    val extractor = mockk<AirbyteHttpRequestFieldExtractor>(relaxed = true)
    every { extractor.contentToJson(any()) } returns Optional.empty()
    val scimHandler = AuthorizationServerHandler(extractor)
    val uris =
      listOf(
        "/scim/v2",
        "/scim/v2?startIndex=1",
        "/scim/v2/Users",
        "/scim/v2/Users?filter=userName%20eq%20%22test%22",
      )

    uris.forEach { uri ->
      val request = buildRequest(uri, """{"password":"secret"}""")
      request.headers().add(AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER, UUID.randomUUID().toString())

      scimHandler.channelRead(context, request)

      assertFalse(request.headers().contains(AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER))
    }
    verify(exactly = 0) { extractor.contentToJson(any()) }
  }

  @Test
  fun `non SCIM lookalike path still extracts authorization fields`() {
    val extractor = mockk<AirbyteHttpRequestFieldExtractor>(relaxed = true)
    every { extractor.contentToJson(any()) } returns Optional.empty()
    val scimHandler = AuthorizationServerHandler(extractor)
    val request = buildRequest("/scim/v20", """{"password":"secret"}""")

    scimHandler.channelRead(context, request)

    verify(exactly = 1) { extractor.contentToJson(any()) }
  }
}
