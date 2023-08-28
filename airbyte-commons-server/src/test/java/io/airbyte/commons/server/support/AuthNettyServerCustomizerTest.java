/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.http.server.netty.NettyServerCustomizer;
import io.micronaut.http.server.netty.NettyServerCustomizer.Registry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test suite for the {@link AuthNettyServerCustomizer} class.
 */
class AuthNettyServerCustomizerTest {

  private static final Integer MAX_CONTENT_LENGTH = 1024;

  private AuthorizationServerHandler authorizationServerHandler;

  private AuthNettyServerCustomizer customizer;

  @BeforeEach
  void setup() {
    authorizationServerHandler = Mockito.mock(AuthorizationServerHandler.class);
    customizer = new AuthNettyServerCustomizer(authorizationServerHandler, MAX_CONTENT_LENGTH);
  }

  @Test
  void testCustomizerRegisteredOnCreation() {
    final BeanCreatedEvent<Registry> event = mock(BeanCreatedEvent.class);
    final Registry registry = mock(Registry.class);
    when(event.getBean()).thenReturn(registry);

    final Registry result = customizer.onCreated(event);
    assertEquals(registry, result);
    verify(registry, times(1)).register(any(NettyServerCustomizer.class));
  }

}
