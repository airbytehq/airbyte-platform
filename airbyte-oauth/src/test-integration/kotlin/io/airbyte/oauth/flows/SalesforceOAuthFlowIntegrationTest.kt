/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.google.common.collect.ImmutableMap
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import io.airbyte.commons.json.Jsons
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.services.OAuthService
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.InetSocketAddress
import java.net.http.HttpClient
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

@Tag("oauth")
class SalesforceOAuthFlowIntegrationTest {
  private lateinit var salesforceOAuthFlow: SalesforceOAuthFlow
  private lateinit var server: HttpServer
  private lateinit var serverHandler: ServerHandler
  private lateinit var httpClient: HttpClient
  private lateinit var oAuthService: OAuthService

  @BeforeEach
  @Throws(IOException::class)
  fun setup() {
    check(Files.exists(CREDENTIALS_PATH)) { "Must provide path to a oauth credentials file." }
    oAuthService = Mockito.mock(OAuthService::class.java)
    httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    salesforceOAuthFlow = SalesforceOAuthFlow(httpClient)

    server = HttpServer.create(InetSocketAddress(8000), 0)
    server.setExecutor(null) // creates a default executor
    server.start()
    serverHandler = ServerHandler("code")
    server.createContext("/code", serverHandler)
  }

  @AfterEach
  fun tearDown() {
    server!!.stop(1)
  }

  @Test
  @Throws(
    InterruptedException::class,
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
  )
  fun testFullSalesforceOAuthFlow() {
    var limit = 20
    val workspaceId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val fullConfigAsString = Files.readString(CREDENTIALS_PATH)
    val credentialsJson = Jsons.deserialize(fullConfigAsString)
    val clientId = credentialsJson["client_id"].asText()
    val sourceOAuthParameter =
      SourceOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withSourceDefinitionId(definitionId)
        .withWorkspaceId(workspaceId)
        .withConfiguration(
          Jsons.jsonNode(
            ImmutableMap
              .builder<Any, Any>()
              .put("client_id", clientId)
              .put("client_secret", credentialsJson["client_secret"].asText())
              .build(),
          ),
        )
    Mockito
      .`when`(oAuthService!!.getSourceOAuthParameterOptional(ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(Optional.of(sourceOAuthParameter))
    val url =
      salesforceOAuthFlow!!.getSourceConsentUrl(
        workspaceId,
        definitionId,
        REDIRECT_URL,
        Jsons.emptyObject(),
        null,
        sourceOAuthParameter.configuration,
      )
    LOGGER.info("Waiting for user consent at: {}", url)
    // TODO: To automate, start a selenium job to navigate to the Consent URL and click on allowing
    // access...
    while (!serverHandler!!.isSucceeded && limit > 0) {
      Thread.sleep(1000)
      limit -= 1
    }
    Assertions.assertTrue(serverHandler!!.isSucceeded, "Failed to get User consent on time")
    val params =
      salesforceOAuthFlow!!.completeSourceOAuth(
        workspaceId,
        definitionId,
        mapOf("code" to serverHandler!!.paramValue!!),
        REDIRECT_URL,
        sourceOAuthParameter.configuration,
      )
    LOGGER.info("Response from completing OAuth Flow is: {}", params.toString())
    Assertions.assertTrue(params.containsKey("refresh_token"))
    Assertions.assertTrue(params["refresh_token"].toString().length > 0)
  }

  internal class ServerHandler(
    private val expectedParam: String,
  ) : HttpHandler {
    var responseQuery: Map<*, *>? = null
      private set
    var paramValue: String? = ""
      private set
    var isSucceeded: Boolean = false
      private set

    override fun handle(t: HttpExchange) {
      val query = t.requestURI.query
      LOGGER.info("Received query: '{}'", query)
      val data: Map<String, String>?
      try {
        data = deserialize(query)
        val response: String
        if (data != null && data.containsKey(expectedParam)) {
          paramValue = data[expectedParam]
          response =
            String.format(
              "Successfully extracted %s:\n'%s'\nTest should be continuing the OAuth Flow to retrieve the refresh_token...",
              expectedParam,
              paramValue,
            )
          responseQuery = data
          LOGGER.info(response)
          t.sendResponseHeaders(200, response.length.toLong())
          isSucceeded = true
        } else {
          response = String.format("Unable to parse query params from redirected url: %s", query)
          t.sendResponseHeaders(500, response.length.toLong())
        }
        val os = t.responseBody
        os.write(response.toByteArray(StandardCharsets.UTF_8))
        os.close()
      } catch (e: RuntimeException) {
        LOGGER.error("Failed to parse from body {}", query, e)
      } catch (e: IOException) {
        LOGGER.error("Failed to parse from body {}", query, e)
      }
    }

    companion object {
      private fun deserialize(query: String?): Map<String, String>? {
        if (query == null) {
          return null
        }
        val result: MutableMap<String, String> = HashMap()
        for (param in query.split("&".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
          val entry = param.split("=".toRegex(), limit = 2).toTypedArray()
          if (entry.size > 1) {
            result[entry[0]] = entry[1]
          } else {
            result[entry[0]] = ""
          }
        }
        return result
      }
    }
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(SalesforceOAuthFlowIntegrationTest::class.java)
    private const val REDIRECT_URL = "http://localhost:8000/code"
    private val CREDENTIALS_PATH: Path = Path.of("secrets/salesforce.json")
  }
}
