/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.requester.assist

import java.io.IOException
import java.net.HttpURLConnection

/**
 * Proxy to the Assist Service. Blocks until the job completes.
 */
interface AssistConfiguration {
  @Throws(IOException::class)
  fun getConnection(path: String): HttpURLConnection
}
