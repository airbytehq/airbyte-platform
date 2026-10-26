/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.http.server.netty.NettyServerCustomizer
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Test suite for the [AuthNettyServerCustomizer] class.
 */
internal class AuthNettyServerCustomizerTest {
  private lateinit var authorizationServerHandler: AuthorizationServerHandler

  private lateinit var customizer: AuthNettyServerCustomizer

  @BeforeEach
  fun setup() {
    authorizationServerHandler = mockk()
    customizer =
      AuthNettyServerCustomizer(
        authorizationServerHandler,
        MAX_CONTENT_LENGTH,
        MAX_INITIAL_LINE_LENGTH,
        MAX_HEADER_SIZE,
        MAX_CHUNK_SIZE,
      )
  }

  @Test
  fun testCustomizerRegisteredOnCreation() {
    val event: BeanCreatedEvent<NettyServerCustomizer.Registry> = mockk()
    val registry: NettyServerCustomizer.Registry = mockk()
    every { event.bean } returns registry
    every { registry.register(any()) } just Runs

    val result = customizer.onCreated(event)
    Assertions.assertEquals(registry, result)
    verify(exactly = 1) { registry.register(any<NettyServerCustomizer>()) }
  }

  companion object {
    private const val MAX_CONTENT_LENGTH = 1024
    private const val MAX_INITIAL_LINE_LENGTH = 4096
    private const val MAX_HEADER_SIZE = 8192
    private const val MAX_CHUNK_SIZE = 8192
  }
}
