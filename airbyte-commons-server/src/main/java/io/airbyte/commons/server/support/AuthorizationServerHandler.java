/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import com.fasterxml.jackson.databind.JsonNode;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Netty {@link ChannelDuplexHandler} that intercepts all operations to ensure that headers
 * required for authorization are populated prior to performing the security check.
 */
@Singleton
@Sharable
public class AuthorizationServerHandler extends ChannelDuplexHandler {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AirbyteHttpRequestFieldExtractor airbyteHttpRequestFieldExtractor;

  private static final Set<String> SKIP_HEADER_UPDATE = Set.of(
      "/api/v1/health",
      // Only update headers if we're not talking about token generation endpoints.
      // Those endpoints don't need the updated headers and can be in a non-JSON format.
      // Did this here because I didn't want to parse a JSON Parsing exception in the contentToJson call.
      "/api/public/v1/applications/token",
      "/api/public/v1/embedded/widget",
      "/api/v1/dataplanes/token");

  public AuthorizationServerHandler(final AirbyteHttpRequestFieldExtractor airbyteHttpRequestFieldExtractor) {
    this.airbyteHttpRequestFieldExtractor = airbyteHttpRequestFieldExtractor;
  }

  @Override
  public void channelRead(
                          final ChannelHandlerContext context,
                          final Object message) {

    Object updatedMessage = message;

    if (message instanceof FullHttpRequest fullHttpRequest) {
      if (!SKIP_HEADER_UPDATE.contains(fullHttpRequest.uri())) {
        updatedMessage = updateHeaders(fullHttpRequest);
      }
    }

    context.fireChannelRead(updatedMessage);
  }

  /**
   * Checks the payload of the raw HTTP request for ID fields that should be copied to an HTTP header
   * in order to facilitate authorization via Micronaut Security.
   *
   * @param httpRequest The raw HTTP request as a {@link FullHttpRequest}.
   * @return The potentially modified raw HTTP request as a {@link FullHttpRequest}.
   */
  protected FullHttpRequest updateHeaders(final FullHttpRequest httpRequest) {
    final String contentAsString = StandardCharsets.UTF_8.decode(httpRequest.content().nioBuffer()).toString();
    final JsonNode contentAsJson = airbyteHttpRequestFieldExtractor.contentToJson(contentAsString).orElse(null);
    for (final AuthenticationId authenticationId : AuthenticationId.values()) {
      log.trace("Checking HTTP request '{}' for field '{}'...", contentAsString, authenticationId.getFieldName());
      final Optional<String> id =
          airbyteHttpRequestFieldExtractor.extractId(contentAsJson, authenticationId.getFieldName());
      if (id.isPresent()) {
        log.trace("Found field '{}' with value '{}' in HTTP request body.", authenticationId.getFieldName(), id.get());
        addHeaderToRequest(authenticationId.getHttpHeader(), id.get(), httpRequest);
      } else {
        log.trace("Field '{}' not found in content.", authenticationId.getFieldName());
      }
    }

    return httpRequest;
  }

  /**
   * Adds the provided header and value to the HTTP request represented by the {@link FullHttpRequest}
   * if the header is not already present.
   *
   * @param headerName The name of the header.
   * @param headerValue The value of the header.
   * @param httpRequest The current HTTP request.
   */
  protected void addHeaderToRequest(final String headerName, final Object headerValue, final FullHttpRequest httpRequest) {
    final HttpHeaders httpHeaders = httpRequest.headers();
    if (!httpHeaders.contains(headerName)) {
      log.trace("Adding HTTP header '{}' with value '{}' to request...", headerName, headerValue);
      httpHeaders.add(headerName, headerValue.toString());
    }
  }

}
