/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.assist

import java.net.HttpURLConnection

/**
 * Proxy to the Assist Service. Blocks until the job completes.
 */
interface AssistConfiguration {
  fun getConnection(path: String): HttpURLConnection
}
