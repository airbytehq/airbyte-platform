/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import io.github.oshai.kotlinlogging.KotlinLogging
import io.netty.channel.ChannelDuplexHandler
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.FullHttpRequest
import jakarta.inject.Singleton
import java.nio.charset.StandardCharsets

/**
 * Custom Netty [ChannelDuplexHandler] that intercepts all operations to ensure that headers
 * required for authorization are populated prior to performing the security check.
 */
@Singleton
@ChannelHandler.Sharable
open class AuthorizationServerHandler(
  private val airbyteHttpRequestFieldExtractor: AirbyteHttpRequestFieldExtractor,
) : ChannelDuplexHandler() {
  override fun channelRead(
    context: ChannelHandlerContext,
    message: Any,
  ) {
    var updatedMessage = message

    if (message is FullHttpRequest) {
      // Always strip client-supplied authentication headers, even for skip-listed URIs.
      // These headers are internal and must only be derived from the request body.
      // Stripping does no JSON parsing, so the non-JSON reason for the skip list does not apply.
      for (authenticationId in AuthenticationId.entries) {
        message.headers().remove(authenticationId.httpHeader)
      }

      if (!SKIP_HEADER_UPDATE.contains(message.uri())) {
        updatedMessage = updateHeaders(message)
      }
    }

    context.fireChannelRead(updatedMessage)
  }

  /**
   * Checks the payload of the raw HTTP request for ID fields that should be copied to an HTTP header
   * in order to facilitate authorization via Micronaut Security.
   *
   * @param httpRequest The raw HTTP request as a [FullHttpRequest].
   * @return The potentially modified raw HTTP request as a [FullHttpRequest].
   */
  protected open fun updateHeaders(httpRequest: FullHttpRequest): FullHttpRequest {
    val contentAsString = StandardCharsets.UTF_8.decode(httpRequest.content().nioBuffer()).toString()
    val contentAsJson = airbyteHttpRequestFieldExtractor.contentToJson(contentAsString).orElse(null)
    for (authenticationId in AuthenticationId.entries) {
      log.trace("Checking HTTP request '{}' for field '{}'...", contentAsString, authenticationId.fieldName)
      val id =
        airbyteHttpRequestFieldExtractor.extractId(contentAsJson, authenticationId.fieldName)
      if (id.isPresent) {
        log.trace("Found field '{}' with value '{}' in HTTP request body.", authenticationId.fieldName, id.get())
        addHeaderToRequest(authenticationId.httpHeader, id.get(), httpRequest)
      } else {
        log.trace("Field '{}' not found in content.", authenticationId.fieldName)
      }
    }

    return httpRequest
  }

  /**
   * Sets the provided header and value on the HTTP request represented by the [FullHttpRequest],
   * always overwriting any existing value to prevent client-supplied header spoofing.
   *
   * @param headerName The name of the header.
   * @param headerValue The value of the header.
   * @param httpRequest The current HTTP request.
   */
  protected fun addHeaderToRequest(
    headerName: String?,
    headerValue: Any,
    httpRequest: FullHttpRequest,
  ) {
    val httpHeaders = httpRequest.headers()
    httpHeaders.set(headerName, headerValue.toString())
    log.trace("Set HTTP header '{}' with value '{}' on request.", headerName, headerValue)
  }

  companion object {
    private val log = KotlinLogging.logger {}

    private val SKIP_HEADER_UPDATE =
      setOf(
        "/api/v1/health", // Only update headers if we're not talking about token generation endpoints.
        // Those endpoints don't need the updated headers and can be in a non-JSON format.
        // Did this here because I didn't want to parse a JSON Parsing exception in the contentToJson call.
        "/api/public/v1/applications/token",
        "/api/public/v1/embedded/widget",
        "/api/v1/dataplanes/token",
      )
  }
}
