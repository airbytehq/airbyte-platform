/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import io.airbyte.data.services.OAuthService
import io.airbyte.oauth.OAuthFlowImplementation
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import java.io.IOException
import java.net.InetSocketAddress
import java.net.http.HttpClient
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

private val log = KotlinLogging.logger {}

@Tag("oauth")
abstract class OAuthFlowIntegrationTest {
  protected lateinit var httpClient: HttpClient
  protected lateinit var oauthService: OAuthService
  protected lateinit var flow: OAuthFlowImplementation
  protected lateinit var server: HttpServer
  protected lateinit var serverHandler: ServerHandler

  protected abstract fun getFlowImplementation(
    oauthService: OAuthService,
    httpClient: HttpClient,
  ): OAuthFlowImplementation

  @BeforeEach
  @Throws(IOException::class)
  open fun setup() {
    check(Files.exists(getCredentialsPath())) { "Must provide path to a oauth credentials file." }
    httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    flow = this.getFlowImplementation(oauthService, httpClient)

    server = HttpServer.create(InetSocketAddress(getServerListeningPort()), 0)
    server.setExecutor(null) // creates a default executor
    server.start()
    serverHandler = ServerHandler("code")
    // Same endpoint as we use for airbyte instance
    server.createContext(getCallBackServerPath(), serverHandler)
  }

  @AfterEach
  fun tearDown() {
    server.stop(1)
  }

  @Throws(InterruptedException::class)
  protected fun waitForResponse(limit: Int) {
    // TODO: To automate, start a selenium job to navigate to the Consent URL and click on allowing
    // access...
    var limit = limit
    while (!serverHandler.isSucceeded && limit > 0) {
      Thread.sleep(1000)
      limit -= 1
    }
  }

  class ServerHandler(
    private val expectedParam: String,
  ) : HttpHandler {
    var paramValue: String = ""
      private set
    var isSucceeded: Boolean = false
      private set

    override fun handle(t: HttpExchange) {
      val query = t.requestURI.query
      log.info("Received query: '{}'", query)
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
          log.info(response)
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
        log.error("Failed to parse from body {}", query, e)
      } catch (e: IOException) {
        log.error("Failed to parse from body {}", query, e)
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

  protected open fun getRedirectUrl(): String = REDIRECT_URL

  protected open fun getServerListeningPort(): Int = SERVER_LISTENING_PORT

  protected open fun getCredentialsPath(): Path = Path.of("secrets/config.json")

  protected fun getCallBackServerPath(): String = "/auth_flow"

  companion object {
    /**
     * Convenience base class for OAuthFlow tests. Those tests right now are meant to be run manually,
     * due to the consent flow in the browser
     */
    const val REDIRECT_URL: String = "http://localhost/auth_flow"
    private const val SERVER_LISTENING_PORT: Int = 80
  }
}
