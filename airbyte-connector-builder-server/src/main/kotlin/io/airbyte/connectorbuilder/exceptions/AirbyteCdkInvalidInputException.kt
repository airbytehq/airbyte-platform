/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.exceptions

import io.airbyte.commons.server.errors.KnownException
import io.airbyte.protocol.models.v0.AirbyteTraceMessage

/**
 * Thrown when the CDK processed the request, but the result contains an error.
 */
class AirbyteCdkInvalidInputException : KnownException {
  var trace: AirbyteTraceMessage? = null

  constructor(message: String, trace: AirbyteTraceMessage) : super(message) {
    this.trace = trace
  }

  constructor(message: String) : super(message)

  override fun getHttpCode(): Int = 422
}
