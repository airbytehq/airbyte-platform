/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.micronaut.context.annotation.Value
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.http.server.netty.NettyServerCustomizer
import io.netty.channel.Channel
import io.netty.handler.codec.http.HttpObjectAggregator
import io.netty.handler.codec.http.HttpRequestDecoder
import jakarta.inject.Singleton

/**
 * Custom Netty customizer that registers the [AuthorizationServerHandler] with the Netty
 * stream pipeline. <br></br>
 * <br></br>
 * This customizer registers the handler as the first in the pipeline to ensure that it can read
 * and, if necessary, modify the incoming HTTP request to include a header that can be used to
 * determine authorization.
 */
@Singleton
class AuthNettyServerCustomizer(
  private val authorizationServerHandler: AuthorizationServerHandler,
  @param:Value("\${micronaut.server.netty.aggregator.max-content-length:52428800}") private val aggregatorMaxContentLength: Int,
  @param:Value("\${micronaut.server.netty.max-initial-line-length:4096}") private val maxInitialLineLength: Int,
  @param:Value("\${micronaut.server.netty.max-header-size:8192}") private val maxHeaderSize: Int,
  @param:Value("\${micronaut.server.netty.max-chunk-size:8192}") private val maxChunkSize: Int,
) : BeanCreatedEventListener<NettyServerCustomizer.Registry> {
  override fun onCreated(event: BeanCreatedEvent<NettyServerCustomizer.Registry>): NettyServerCustomizer.Registry {
    val registry = event.bean
    registry.register(Customizer(null)) //
    return registry
  }

  /**
   * Custom [NettyServerCustomizer] that registers the [AuthorizationServerHandler] as the
   * first handler in the Netty pipeline.
   */
  private inner class Customizer(
    private val channel: Channel?,
  ) : NettyServerCustomizer {
    override fun specializeForChannel(
      channel: Channel,
      role: NettyServerCustomizer.ChannelRole,
    ): NettyServerCustomizer = Customizer(channel)

    override fun onStreamPipelineBuilt() {
            /*
             * Register the handlers in reverse order so that the final order is: 1. Decoder 2. Aggregator 3.
             * Authorization Handler
             *
             * This is to ensure that the full HTTP request with content is provided to the authorization
             * handler.
             */
      channel!!
        .pipeline()
        .addFirst("authorizationServerHandler", authorizationServerHandler)
        .addFirst("aggregator", HttpObjectAggregator(aggregatorMaxContentLength))
        .addFirst("decoder", HttpRequestDecoder(maxInitialLineLength, maxHeaderSize, maxChunkSize))
    }
  }
}
