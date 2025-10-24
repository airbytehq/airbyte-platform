/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.builder.assist.AssistConfiguration
import io.airbyte.commons.server.builder.assist.AssistProxy
import io.airbyte.commons.server.builder.exceptions.ConnectorBuilderException
import jakarta.inject.Inject
import jakarta.inject.Singleton

/**
 * Proxy to the Assist API.
 */
@Singleton
class AssistProxyHandler
  @Inject
  constructor(
    private val proxyConfig: AssistConfiguration,
  ) {
    /**
     * Call the Assistant to get connector data
     */
    fun process(
      requestBody: Map<String, Any>,
      waitForResponse: Boolean = true,
    ): Map<String, Any> {
      val path = "/v1/process"
      val proxy = AssistProxy(this.proxyConfig)

      val jsonBody = Jsons.jsonNode(requestBody)
      return if (waitForResponse) {
        val result = proxy.post(path, jsonBody)
        val response = Jsons.`object`(result, ProxyResponse::class.java) as ProxyResponse
        response.tree = result
        response
      } else {
        proxy.postNoWait(path, jsonBody)
        mapOf("status" to "sent")
      }
    }
  }

@JsonSerialize(using = ProxyResponseSerializer::class)
class ProxyResponse : HashMap<String, Any>() {
  var tree: JsonNode? = null
}

/*
 * Let's make sure we include everything including empty arrays and empty strings by using the node tree directly
 */
class ProxyResponseSerializer : StdSerializer<ProxyResponse?> {
  constructor() : this(null)
  constructor(t: Class<ProxyResponse?>?) : super(t)

  override fun serialize(
    value: ProxyResponse?,
    gen: JsonGenerator?,
    provider: SerializerProvider?,
  ) {
    objectMapper.writeTree(gen, value?.tree)
  }

  companion object {
    private val objectMapper = ObjectMapper()
  }
}
