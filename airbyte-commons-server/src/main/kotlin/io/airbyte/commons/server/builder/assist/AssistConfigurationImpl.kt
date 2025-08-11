/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.assist

import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import org.jooq.tools.StringUtils
import java.io.IOException
import java.net.HttpURLConnection
import java.net.MalformedURLException
import java.net.ProtocolException
import java.net.URL

/**
 * Construct and send requests to the CDK's Connector Builder handler.
 */
@Singleton
class AssistConfigurationImpl(
  @Value("\${airbyte.connector-builder-server.ai-assist.url-base}") private val targetApiBaseUrl: String,
) : AssistConfiguration {
  @Throws(IOException::class)
  override fun getConnection(path: String): HttpURLConnection {
    if (StringUtils.isBlank(targetApiBaseUrl)) {
      throw RuntimeException("Assist Service URL is not set.")
    }
    try {
      val url = URL("$targetApiBaseUrl$path")
      val connection = url.openConnection() as HttpURLConnection
      return connection
    } catch (e: ProtocolException) {
      throw RuntimeException(e)
    } catch (e: MalformedURLException) {
      throw RuntimeException(e)
    }
  }
}
