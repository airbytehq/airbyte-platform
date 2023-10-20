package io.airbyte.api.server.netty

import io.micronaut.json.tree.JsonNode
import io.netty.buffer.ByteBuf
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.HttpUtil
import io.netty.handler.codec.http.QueryStringDecoder
import org.jooq.tools.json.JSONObject
import java.util.Locale
import java.util.UUID

class NettyHttpRequest(private val request: HttpRequest) {
  val requestId: String = UUID.randomUUID().toString()
  private var writer: CaptureWriter? = null

  init {
    request.headers().add("X-Airbyte-Request-Id", requestId)
  }

  fun register(writer: CaptureWriter) {
    this.writer = writer
  }

  fun buffer(content: ByteBuf?) {
    // TODO may need to make this thread safe
    writer!!.writeRequest(content!!)
  }

  val headers: Map<String, List<String>>?
    get() =
      if (request.headers() != null) {
        request.headers().entries().associate { entry -> entry.key.lowercase(Locale.getDefault()) to listOf(entry.value) }
      } else {
        null
      }

  val contentType: String?
    get() {
      return if (request.headers() != null) {
        request.headers().get(HttpHeaderNames.CONTENT_TYPE)
      } else {
        null
      }
    }

  fun getBodyText(droppedText: String): String {
    return if (writer!!.isReqValid) {
      String(writer!!.getReqBuffer())
    } else {
      droppedText
    }
  }

  val queryString: String
    get() {
      val queryStringDecoder = QueryStringDecoder(request.uri())
      return queryStringDecoder.rawQuery()
    }
  val contentLength: Long
    get() = HttpUtil.getContentLength(request, 0L)
  val protocol: String
    get() = request.protocolVersion().text()
  val method: String
    get() = request.method().name()
  val requestURI: String
    get() = request.uri()

  fun getLogString(): String {
    val logMap =
      mapOf(
        Pair("queryString", queryString),
        Pair("method", method),
        Pair("requestURI", requestURI),
        Pair("bodyText", maskBody(getBodyText("...dropped..."), contentType)),
        Pair("headers", maskHeaders(headers)),
      )
    return JSONObject(logMap as MutableMap<String, JsonNode>?).toString()
  }
}
