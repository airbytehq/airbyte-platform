/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.handlers

import io.airbyte.commons.json.Jsons
import io.airbyte.connector_builder.exceptions.ConnectorBuilderException
import io.airbyte.connector_builder.requester.assist.AssistConfiguration
import io.airbyte.connector_builder.requester.assist.AssistProxy
import jakarta.inject.Inject
import jakarta.inject.Singleton

/**
 * Proxy to the Assist API.
 */
@Singleton
class AssistProxyHandler
  @Inject
  constructor(private val proxyConfig: AssistConfiguration) {
    /**
     * Call the Assistant to get connector data
     */
    @Throws(ConnectorBuilderException::class)
    fun process(requestBody: Map<String, Object>): Map<String, Object> {
      val path = "/v1/process"
      val proxy = AssistProxy(this.proxyConfig)

      val jsonBody = Jsons.jsonNode(requestBody)
      val result = proxy.post(path, jsonBody)
      return Jsons.`object`(result, Map::class.java) as Map<String, Object>
    }
  }
