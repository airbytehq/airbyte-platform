/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.requester.assist

import java.io.IOException
import java.net.HttpURLConnection

/**
 * Proxy to the Assist Service. Blocks until the job completes.
 */
interface AssistConfiguration {
  @Throws(IOException::class)
  fun getConnection(path: String): HttpURLConnection
}
