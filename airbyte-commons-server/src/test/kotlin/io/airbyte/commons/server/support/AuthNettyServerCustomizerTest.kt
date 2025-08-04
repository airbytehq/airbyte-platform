/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.http.server.netty.NettyServerCustomizer
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito

/**
 * Test suite for the [AuthNettyServerCustomizer] class.
 */
internal class AuthNettyServerCustomizerTest {
  private lateinit var authorizationServerHandler: AuthorizationServerHandler

  private lateinit var customizer: AuthNettyServerCustomizer

  @BeforeEach
  fun setup() {
    authorizationServerHandler = Mockito.mock(AuthorizationServerHandler::class.java)
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
    val event: BeanCreatedEvent<NettyServerCustomizer.Registry> =
      Mockito.mock(
        BeanCreatedEvent::class.java,
      ) as BeanCreatedEvent<NettyServerCustomizer.Registry>
    val registry = Mockito.mock(NettyServerCustomizer.Registry::class.java)
    Mockito.`when`(event.bean).thenReturn(registry)

    val result = customizer.onCreated(event)
    Assertions.assertEquals(registry, result)
    Mockito
      .verify(registry, Mockito.times(1))
      .register(ArgumentMatchers.any(NettyServerCustomizer::class.java))
  }

  companion object {
    private const val MAX_CONTENT_LENGTH = 1024
    private const val MAX_INITIAL_LINE_LENGTH = 4096
    private const val MAX_HEADER_SIZE = 8192
    private const val MAX_CHUNK_SIZE = 8192
  }
}
