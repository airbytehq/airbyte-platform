package io.airbyte.server.apis.publicapi.netty

import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.http.netty.channel.ChannelPipelineCustomizer
import io.micronaut.http.server.netty.NettyServerCustomizer
import io.netty.channel.Channel
import jakarta.inject.Singleton

@Singleton
class LoggingNettyServerCustomizer() : BeanCreatedEventListener<NettyServerCustomizer.Registry> {
  override fun onCreated(event: BeanCreatedEvent<NettyServerCustomizer.Registry>): NettyServerCustomizer.Registry {
    val registry: NettyServerCustomizer.Registry = event.bean
    registry.register(Customizer(null))
    return registry
  }

  private inner class Customizer(private val channel: Channel?) : NettyServerCustomizer {
    override fun specializeForChannel(
      channel: Channel,
      role: NettyServerCustomizer.ChannelRole,
    ): NettyServerCustomizer {
      return Customizer(channel)
    }

    override fun onStreamPipelineBuilt() {
      channel!!.pipeline().addAfter(
        ChannelPipelineCustomizer.HANDLER_HTTP_SERVER_CODEC,
        "AirbyteApiLogs",
        LoggingNettyChannelHandler(),
      )
    }
  }
}
