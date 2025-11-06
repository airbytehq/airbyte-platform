/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.assist

import io.airbyte.micronaut.runtime.AirbyteConnectorBuilderConfig
import jakarta.inject.Singleton
import org.jooq.tools.StringUtils
import java.net.HttpURLConnection
import java.net.MalformedURLException
import java.net.ProtocolException
import java.net.URL

/**
 * Construct and send requests to the CDK's Connector Builder handler.
 */
@Singleton
class AssistConfigurationImpl(
  private val airbyteConnectorBuilderConfig: AirbyteConnectorBuilderConfig,
) : AssistConfiguration {
  override fun getConnection(path: String): HttpURLConnection {
    if (StringUtils.isBlank(airbyteConnectorBuilderConfig.aiAssist.urlBase)) {
      throw RuntimeException("Assist Service URL is not set.")
    }
    try {
      val url = URL("${airbyteConnectorBuilderConfig.aiAssist.urlBase}$path")
      val connection = url.openConnection() as HttpURLConnection
      return connection
    } catch (e: ProtocolException) {
      throw RuntimeException(e)
    } catch (e: MalformedURLException) {
      throw RuntimeException(e)
    }
  }
}
