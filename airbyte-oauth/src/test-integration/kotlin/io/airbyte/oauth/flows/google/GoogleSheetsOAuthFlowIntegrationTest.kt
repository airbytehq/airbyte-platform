/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows.google

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import io.airbyte.commons.json.Jsons
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.services.OAuthService
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.REFRESH_TOKEN_KEY
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.io.IOException
import java.net.InetSocketAddress
import java.net.http.HttpClient
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

@Tag("oauth")
class GoogleSheetsOAuthFlowIntegrationTest {
  private lateinit var googleSheetsOAuthFlow: GoogleSheetsOAuthFlow
  private lateinit var server: HttpServer
  private lateinit var serverHandler: ServerHandler
  private lateinit var httpClient: HttpClient
  private lateinit var oAuthService: OAuthService

  @BeforeEach
  fun setup() {
    check(Files.exists(CREDENTIALS_PATH)) { "Must provide path to a oauth credentials file." }
    oAuthService = Mockito.mock(OAuthService::class.java)
    httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    googleSheetsOAuthFlow = GoogleSheetsOAuthFlow(httpClient)

    server = HttpServer.create(InetSocketAddress(80), 0)
    server.setExecutor(null) // creates a default executor
    server.start()
    serverHandler = ServerHandler(AUTH_CODE_KEY)
    server.createContext("/code", serverHandler)
  }

  @AfterEach
  fun tearDown() {
    server.stop(1)
  }

  @Test
  fun testFullGoogleOAuthFlow() {
    var limit = 20
    val workspaceId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val fullConfigAsString = Files.readString(CREDENTIALS_PATH)
    val credentialsJson = Jsons.deserialize(fullConfigAsString)
    val sourceOAuthParameter =
      SourceOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withSourceDefinitionId(definitionId)
        .withWorkspaceId(workspaceId)
        .withConfiguration(
          Jsons.jsonNode(
            mapOf(
              "credentials" to
                mapOf(
                  CLIENT_ID_KEY to credentialsJson["credentials"][CLIENT_ID_KEY].asText(),
                  CLIENT_SECRET_KEY to credentialsJson["credentials"][CLIENT_SECRET_KEY].asText(),
                ),
            ),
          ),
        )
    Mockito
      .`when`(oAuthService.getSourceOAuthParameterOptional(ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(Optional.of(sourceOAuthParameter))
    val url =
      googleSheetsOAuthFlow.getSourceConsentUrl(
        workspaceId,
        definitionId,
        REDIRECT_URL,
        Jsons.emptyObject(),
        null,
        sourceOAuthParameter.configuration,
      )
    log.info { "Waiting for user consent at: $url" }
    // TODO: To automate, start a selenium job to navigate to the Consent URL and click on allowing
    // access...
    while (!serverHandler.isSucceeded && limit > 0) {
      Thread.sleep(1000)
      limit -= 1
    }
    assertTrue(serverHandler.isSucceeded, "Failed to get User consent on time")
    val params =
      googleSheetsOAuthFlow.completeSourceOAuth(
        workspaceId,
        definitionId,
        mapOf(AUTH_CODE_KEY to serverHandler.paramValue),
        REDIRECT_URL,
        sourceOAuthParameter.configuration,
      )
    log.info { "Response from completing OAuth Flow is: $params" }
    assertTrue(params.containsKey("credentials"))
    val credentials = params["credentials"] as Map<*, *>?
    assertTrue(credentials!!.containsKey(REFRESH_TOKEN_KEY))
    assertTrue(credentials[REFRESH_TOKEN_KEY].toString().isNotEmpty())
    assertTrue(credentials.containsKey("access_token"))
    assertTrue(credentials["access_token"].toString().isNotEmpty())
  }

  internal class ServerHandler(
    private val expectedParam: String,
  ) : HttpHandler {
    var paramValue: String = ""
      private set
    var isSucceeded: Boolean = false
      private set

    override fun handle(t: HttpExchange) {
      val query = t.requestURI.query
      log.info { "Received query: '$query'" }
      val data: Map<String, String>?
      try {
        data = deserialize(query)
        val response: String
        if (data != null && data.containsKey(expectedParam)) {
          paramValue = data[expectedParam]!!
          response =
            String.format(
              "Successfully extracted %s:\n'%s'\nTest should be continuing the OAuth Flow to retrieve the refresh_token...",
              expectedParam,
              paramValue,
            )
          log.info { response }
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
        log.error(e) { "Failed to parse from body $query" }
      } catch (e: IOException) {
        log.error(e) { "Failed to parse from body $query" }
      }
    }

    companion object {
      private fun deserialize(query: String?): Map<String, String>? {
        if (query == null) {
          return null
        }
        val result: MutableMap<String, String> = HashMap()
        for (param in query.split("&".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
          val entry = param.split("=".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
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
    private val log = KotlinLogging.logger {}
    private const val REDIRECT_URL = "http://localhost/code"
    private val CREDENTIALS_PATH: Path = Path.of("secrets/google_sheets.json")
  }
}
