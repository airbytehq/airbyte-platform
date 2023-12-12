package io.airbyte.api.server.netty

import io.micronaut.json.tree.JsonNode
import io.netty.buffer.ByteBuf
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpResponse
import io.netty.handler.codec.http.HttpUtil
import org.jooq.tools.json.JSONObject
import java.util.Locale

class NettyHttpResponse {
  private var response: HttpResponse? = null
  private var writer: CaptureWriter? = null

  constructor(response: HttpResponse?) {
    this.response = response
  }

  fun register(writer: CaptureWriter?) {
    this.writer = writer
  }

  fun buffer(content: ByteBuf?) {
    // TODO may need to make this thread safe
    writer!!.writeResponse(content!!)
  }

  fun getHeaders(): Map<String, List<String>>? {
    return if (response!!.headers() != null) {
      response!!.headers().entries().associate { entry -> entry.key.lowercase(Locale.getDefault()) to listOf(entry.value) }
    } else {
      null
    }
  }

  fun getContentType(): String? {
    return if (response!!.headers() != null) {
      response!!.headers().get(HttpHeaderNames.CONTENT_TYPE)
    } else {
      null
    }
  }

  fun getBodyText(droppedText: String): String {
    return if (writer!!.isResValid) {
      String(writer!!.getResBuffer())
    } else {
      droppedText
    }
  }

  fun getContentLength(originalSize: Boolean): Long {
    if (writer!!.isResValid || originalSize) {
      val size: Long = HttpUtil.getContentLength(response, -1L)
      if (size > 0) {
        return size
      }
    }
    return -1L
  }

  fun getStatus(): Int {
    return response!!.status().code()
  }

  fun getLocationHeader(): String? {
    return if (response!!.headers() != null) {
      response!!.headers().get(HttpHeaderNames.LOCATION)
    } else {
      null
    }
  }

  fun getLogString(): String {
    val logMap =
      mapOf(
        Pair("status", getStatus()),
        Pair("bodyText", maskBody(getBodyText("...dropped..."), getContentType())),
        Pair("headers", maskHeaders(getHeaders())),
      )
    return JSONObject(logMap as MutableMap<String, JsonNode>?).toString()
  }
}
