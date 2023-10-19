package io.airbyte.api.server.netty

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled

private const val MAX_BUFFER_SIZE = 1 * 1024 * 1024

class CaptureWriter {
  private val reqBuffer = Unpooled.buffer()
  private val resBuffer = Unpooled.buffer()
  var isReqValid = true
  var isResValid = true

  fun writeRequest(content: ByteBuf) {
    if (content == Unpooled.EMPTY_BUFFER) {
      return
    }
    val readIndex = content.readerIndex()
    val readableBytes = content.readableBytes()
    if (reqBuffer.readableBytes() + resBuffer.readableBytes() + readableBytes > MAX_BUFFER_SIZE) {
      isReqValid = false
    } else if (isReqValid) {
      reqBuffer.ensureWritable(readableBytes)
      content.readBytes(reqBuffer, readableBytes)
      content.readerIndex(readIndex)
    }
  }

  fun writeResponse(content: ByteBuf) {
    if (content == Unpooled.EMPTY_BUFFER) {
      return
    }
    val readIndex = content.readerIndex()
    val readableBytes = content.readableBytes()
    if (reqBuffer.readableBytes() + resBuffer.readableBytes() + readableBytes > MAX_BUFFER_SIZE) {
      isResValid = false
    } else if (isResValid) {
      resBuffer.ensureWritable(readableBytes)
      content.readBytes(resBuffer, readableBytes)
      content.readerIndex(readIndex)
    }
  }

  fun getReqBuffer(): ByteArray {
    val length = reqBuffer.readableBytes()
    if (length == reqBuffer.capacity()) {
      return reqBuffer.array()
    }
    val target = ByteArray(length)
    System.arraycopy(reqBuffer.array(), 0, target, 0, length)
    return target
  }

  fun getResBuffer(): ByteArray {
    val length = resBuffer.readableBytes()
    if (length == resBuffer.capacity()) {
      return resBuffer.array()
    }
    val target = ByteArray(length)
    System.arraycopy(resBuffer.array(), 0, target, 0, length)
    return target
  }
}
