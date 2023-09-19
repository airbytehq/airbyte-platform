/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import io.micronaut.context.annotation.Value;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.micronaut.http.server.netty.NettyServerCustomizer;
import io.micronaut.http.server.netty.NettyServerCustomizer.Registry;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Custom Netty customizer that registers the {@link AuthorizationServerHandler} with the Netty
 * stream pipeline. <br />
 * <br />
 * This customizer registers the handler as the first in the pipeline to ensure that it can read
 * and, if necessary, modify the incoming HTTP request to include a header that can be used to
 * determine authorization.
 */
@Singleton
@Slf4j
public class AuthNettyServerCustomizer implements BeanCreatedEventListener<Registry> {

  private final AuthorizationServerHandler authorizationServerHandler;

  private final Integer aggregatorMaxContentLength;

  public AuthNettyServerCustomizer(final AuthorizationServerHandler authorizationServerHandler,
                                   @Value("${micronaut.server.netty.aggregator.max-content-length}") final Integer aggregatorMaxContentLength) {
    this.authorizationServerHandler = authorizationServerHandler;
    this.aggregatorMaxContentLength = aggregatorMaxContentLength;
  }

  @Override
  public Registry onCreated(final BeanCreatedEvent<Registry> event) {
    final NettyServerCustomizer.Registry registry = event.getBean();
    registry.register(new Customizer(null)); //
    return registry;
  }

  /**
   * Custom {@link NettyServerCustomizer} that registers the {@link AuthorizationServerHandler} as the
   * first handler in the Netty pipeline.
   */
  private class Customizer implements NettyServerCustomizer {

    private final Channel channel;

    Customizer(final Channel channel) {
      this.channel = channel;
    }

    @Override
    public NettyServerCustomizer specializeForChannel(final Channel channel, final ChannelRole role) {
      return new Customizer(channel);
    }

    @Override
    public void onStreamPipelineBuilt() {
      /*
       * Register the handlers in reverse order so that the final order is: 1. Decoder 2. Aggregator 3.
       * Authorization Handler
       *
       * This is to ensure that the full HTTP request with content is provided to the authorization
       * handler.
       */
      channel.pipeline()
          .addFirst("authorizationServerHandler", authorizationServerHandler)
          .addFirst("aggregator", new HttpObjectAggregator(aggregatorMaxContentLength))
          .addFirst("decoder", new HttpRequestDecoder());
    }

  }

}
