/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.EnterpriseSourceStub
import io.airbyte.api.model.generated.EnterpriseSourceStubsReadList
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Request
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration

@Singleton
open class EnterpriseSourceStubsHandler(
  @Value("\${airbyte.connector-registry.enterprise.enterprise-source-stubs-url}")
  private val enterpriseSourceStubsUrl: String,
  @Value("\${airbyte.connector-registry.remote.timeout-ms}")
  private val remoteTimeoutMs: Long,
) {
  private val logger = LoggerFactory.getLogger(this::class.java)
  private val okHttpClient: OkHttpClient =
    OkHttpClient.Builder()
      .callTimeout(Duration.ofMillis(remoteTimeoutMs))
      .build()

  @Throws(IOException::class)
  fun listEnterpriseSourceStubs(): EnterpriseSourceStubsReadList {
    return try {
      val request =
        Request.Builder()
          .url(enterpriseSourceStubsUrl)
          .build()

      okHttpClient.newCall(request).execute().use { response ->
        if (!response.isSuccessful) throw IOException("Unexpected code ${response.code}")

        val jsonResponse = response.body?.string() ?: throw IOException("Empty response body")

        val objectMapper = ObjectMapper()
        val typeReference = object : TypeReference<List<EnterpriseSourceStub>>() {}
        val stubs: List<EnterpriseSourceStub> = objectMapper.readValue(jsonResponse, typeReference)

        EnterpriseSourceStubsReadList().apply {
          enterpriseSourceStubs = stubs
        }
      }
    } catch (error: IOException) {
      logger.error(
        "Encountered an HTTP error fetching enterprise connectors. Message: {}",
        error.message,
      )
      throw IOException("HTTP error fetching enterprise sources", error)
    } catch (error: Exception) {
      logger.error("Unexpected error fetching enterprise sources", error)
      throw IOException("Encountered an unexpected error fetching enterprise sources", error)
    }
  }
}
