package io.airbyte.api.server.netty

import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.http.netty.channel.ChannelPipelineCustomizer
import io.micronaut.http.server.netty.NettyServerCustomizer
import io.netty.channel.Channel

@jakarta.inject.Singleton
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
      channel!!.pipeline().addBefore(
        ChannelPipelineCustomizer.HANDLER_HTTP_STREAM,
        "AirbyteApiLogs",
        LoggingNettyChannelHandler(),
      )
    }
  }
}
