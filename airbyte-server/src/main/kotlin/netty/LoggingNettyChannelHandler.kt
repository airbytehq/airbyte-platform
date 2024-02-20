package io.airbyte.api.server.netty

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelDuplexHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelPromise
import io.netty.handler.codec.http.HttpContent
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.HttpResponse
import io.netty.handler.codec.http.LastHttpContent
import org.slf4j.LoggerFactory

class LoggingNettyChannelHandler : ChannelDuplexHandler() {
  private val droppedText = "[...dropped...]"
  companion object {
    private val log = LoggerFactory.getLogger(LoggingNettyChannelHandler::class.java)
  }

  private var request: NettyHttpRequest? = null
  private var response: NettyHttpResponse? = null
  private var writer: CaptureWriter? = null
  private var requestCaptured = false
  private var responseCaptured = false
  private var captured = false

  override fun channelRead(
    context: ChannelHandlerContext,
    message: Any,
  ) {
    if (HttpRequest::class.java.isInstance(message)) {
      captured = false
      requestCaptured = false
      responseCaptured = false
      writer = CaptureWriter()
      request = NettyHttpRequest(message as HttpRequest)
      request!!.register(writer!!)
      log.info("[{}] {}", request!!.method, request!!.requestURI)
    }
    if (request == null) {
      return
    }
    if (HttpContent::class.java.isInstance(message)) {
      request!!.buffer((message as HttpContent).content())
    }
    if (ByteBuf::class.java.isInstance(message)) {
      request!!.buffer(message as ByteBuf)
    }
    if (LastHttpContent::class.java.isInstance(message)) {
      requestCaptured = true
      if (!captured && responseCaptured) {
        capture()
      }
    }
    context.fireChannelRead(message)
  }

  override fun write(
    context: ChannelHandlerContext,
    message: Any,
    promise: ChannelPromise?,
  ) {
    if (HttpResponse::class.java.isInstance(message)) {
      val httpResponse: HttpResponse = message as HttpResponse
      response = NettyHttpResponse(httpResponse)
      writer = CaptureWriter()
      response!!.register(writer)
    }
    if (response == null) {
      return
    }
    if (HttpContent::class.java.isInstance(message)) {
      response!!.buffer((message as HttpContent).content())
    }
    if (ByteBuf::class.java.isInstance(message)) {
      response!!.buffer(message as ByteBuf)
    }
    if (LastHttpContent::class.java.isInstance(message)) {
      responseCaptured = true
      if (!captured && requestCaptured) {
        capture()
      }
    }
    context.write(message, promise)
  }

  private fun capture() {
    log.info("Request: [{}] -- {}", request!!.requestId, request!!.getLogString())
    log.info("Response: [{}] -- {}", request!!.requestId, response!!.getLogString())
    request = null
    response = null
    writer = null
  }
}
