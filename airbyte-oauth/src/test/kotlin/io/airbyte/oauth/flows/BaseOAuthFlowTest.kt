/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.services.OAuthService
import io.airbyte.oauth.AUTH_CODE_KEY
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.CLIENT_ID_KEY
import io.airbyte.oauth.CLIENT_SECRET_KEY
import io.airbyte.oauth.MoreOAuthParameters
import io.airbyte.oauth.REFRESH_TOKEN_KEY
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.io.IOException
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.Optional
import java.util.UUID

abstract class BaseOAuthFlowTest {
  protected lateinit var httpClient: HttpClient
  private lateinit var oAuthService: OAuthService
  private lateinit var oauthFlow: BaseOAuthFlow

  private lateinit var workspaceId: UUID
  private lateinit var definitionId: UUID
  private lateinit var sourceOAuthParameter: SourceOAuthParameter
  private lateinit var destinationOAuthParameter: DestinationOAuthParameter

  @BeforeEach
  fun setup() {
    httpClient = Mockito.mock(HttpClient::class.java)
    oAuthService = Mockito.mock(OAuthService::class.java)
    oauthFlow = oAuthFlow

    workspaceId = UUID.randomUUID()
    definitionId = UUID.randomUUID()
    sourceOAuthParameter =
      SourceOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withSourceDefinitionId(definitionId)
        .withConfiguration(oAuthParamConfig)
    Mockito
      .`when`(oAuthService.getSourceOAuthParameterOptional(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull()))
      .thenReturn(Optional.of(sourceOAuthParameter))
    destinationOAuthParameter =
      DestinationOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withDestinationDefinitionId(definitionId)
        .withConfiguration(oAuthParamConfig)
    Mockito
      .`when`(oAuthService.getDestinationOAuthParameterOptional(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull()))
      .thenReturn(Optional.of(destinationOAuthParameter))
  }

  /**
   * This should be implemented for the particular oauth flow implementation.
   *
   * @return the oauth flow implementation to test
   */
  protected abstract val oAuthFlow: BaseOAuthFlow

  /**
   * This should be implemented for the particular oauth flow implementation.
   *
   * @return the expected consent URL
   */
  protected abstract val expectedConsentUrl: String

  protected open val expectedOutput: Map<String, String>
    /**
     * Redefine if the oauth flow implementation does not return `refresh_token`. (maybe for example
     * using `access_token` like in the `GithubOAuthFlowTest` instead?).
     *
     * @return the full output expected to be returned by this oauth flow + all its instance wide
     * variables
     */
    get() =
      mapOf(
        REFRESH_TOKEN_KEY to "refresh_token_response",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
        CLIENT_SECRET_KEY to MoreOAuthParameters.SECRET_MASK,
      )

  protected open val completeOAuthOutputSpecification: JsonNode
    /**
     * Redefine if the oauth flow implementation does not return `refresh_token`. (maybe for example
     * using `access_token` like in the `GithubOAuthFlowTest` instead?)
     *
     * @return the output specification used to identify what the oauth flow should be returning
     */
    get() =
      getJsonSchema(
        mapOf(
          REFRESH_TOKEN_KEY to mapOf(TYPE to "string"),
        ),
      )

  protected open val expectedFilteredOutput: Map<String, String>
    /**
     * Redefine if the oauth flow implementation does not return `refresh_token`. (maybe for example
     * using `access_token` like in the `GithubOAuthFlowTest` instead?)
     *
     * @return the filtered outputs once it is filtered by the output specifications
     */
    get() =
      mapOf(
        REFRESH_TOKEN_KEY to "refresh_token_response",
        CLIENT_ID_KEY to MoreOAuthParameters.SECRET_MASK,
      )

  protected val completeOAuthServerOutputSpecification: JsonNode
        /*
         * @return the output specification used to filter what the oauth flow should be returning
         */
    get() =
      getJsonSchema(
        mapOf(
          CLIENT_ID_KEY to mapOf(TYPE to "string"),
        ),
      )

  protected open val expectedOutputPath: List<String?>
    /**
     * Redefine to match the oauth implementation flow getDefaultOAuthOutputPath().
     *
     * @return the backward compatible path that is used in the deprecated oauth flows.
     */
    get() = listOf("credentials")

    /*
     * @return if the OAuth implementation flow has a dependency on input values from connector config.
     */
  protected fun hasDependencyOnConnectorConfigValues(): Boolean = !inputOAuthConfiguration.isEmpty

  protected open val inputOAuthConfiguration: JsonNode
    /**
     * If the OAuth implementation flow has a dependency on input values from connector config, this
     * method should be redefined.
     *
     * @return the input configuration sent to oauth flow (values from connector config)
     */
    get() = Jsons.emptyObject()

  protected open val userInputFromConnectorConfigSpecification: JsonNode
    /**
     * If the OAuth implementation flow has a dependency on input values from connector config, this
     * method should be redefined.
     *
     * @return the input configuration sent to oauth flow (values from connector config)
     */
    get() = getJsonSchema(emptyMap())

  protected open val oAuthParamConfig: JsonNode?
        /*
         * @return the instance wide config params for this oauth flow
         */
    get() =
      Jsons.jsonNode(
        mapOf(
          CLIENT_ID_KEY to "test_client_id",
          CLIENT_SECRET_KEY to "test_client_secret",
        ),
      )

  protected fun getoAuthConfigSpecification(): OAuthConfigSpecification =
    OAuthConfigSpecification()
      .withOauthUserInputFromConnectorConfigSpecification(userInputFromConnectorConfigSpecification)
      .withCompleteOauthOutputSpecification(completeOAuthOutputSpecification)
      .withCompleteOauthServerOutputSpecification(completeOAuthServerOutputSpecification)

  private val emptyOAuthConfigSpecification: OAuthConfigSpecification
    get() =
      OAuthConfigSpecification()
        .withCompleteOauthOutputSpecification(Jsons.emptyObject())
        .withCompleteOauthServerOutputSpecification(Jsons.emptyObject())

  protected val constantState: String
    get() = "state"

  protected open val queryParams: Map<String, Any>
    get() =
      mapOf(
        AUTH_CODE_KEY to TEST_CODE,
        STATE to constantState,
      )

  protected open val mockedResponse: String
    get() {
      val returnedCredentials = expectedOutput
      return Jsons.serialize(returnedCredentials)
    }

  protected open val oAuthConfigSpecification: OAuthConfigSpecification
    get() =
      getoAuthConfigSpecification() // change property types to induce json validation errors.
        .withCompleteOauthServerOutputSpecification(
          getJsonSchema(
            mapOf(
              CLIENT_ID_KEY to mapOf(TYPE to "integer"),
            ),
          ),
        ).withCompleteOauthOutputSpecification(
          getJsonSchema(
            mapOf(
              REFRESH_TOKEN_KEY to mapOf(TYPE to "integer"),
            ),
          ),
        )

  @Test
  fun testGetDefaultOutputPath() {
    assertEquals(expectedOutputPath, oauthFlow.getDefaultOAuthOutputPath())
  }

  @Test
  fun testValidateInputOAuthConfigurationFailure() {
    val invalidInputOAuthConfiguration = Jsons.jsonNode(mapOf("UnexpectedRandomField" to 42))
    assertThrows(
      JsonValidationException::class.java,
    ) {
      oauthFlow.getSourceConsentUrl(
        workspaceId,
        definitionId,
        REDIRECT_URL,
        invalidInputOAuthConfiguration,
        getoAuthConfigSpecification(),
        Jsons.emptyObject(),
      )
    }
    assertThrows(
      JsonValidationException::class.java,
    ) {
      oauthFlow.getDestinationConsentUrl(
        workspaceId,
        definitionId,
        REDIRECT_URL,
        invalidInputOAuthConfiguration,
        getoAuthConfigSpecification(),
        Jsons.emptyObject(),
      )
    }
    assertThrows(
      JsonValidationException::class.java,
    ) {
      oauthFlow.completeSourceOAuth(
        workspaceId,
        definitionId,
        emptyMap(),
        REDIRECT_URL,
        invalidInputOAuthConfiguration,
        getoAuthConfigSpecification(),
        sourceOAuthParameter.configuration,
      )
    }
    assertThrows(
      JsonValidationException::class.java,
    ) {
      oauthFlow.completeDestinationOAuth(
        workspaceId,
        definitionId,
        emptyMap(),
        REDIRECT_URL,
        invalidInputOAuthConfiguration,
        getoAuthConfigSpecification(),
        destinationOAuthParameter.configuration,
      )
    }
  }

  @Test
  fun testGetConsentUrlEmptyOAuthParameters() {
    Mockito
      .`when`(
        oAuthService.getSourceOAuthParameterOptional(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull()),
      ).thenReturn(Optional.empty())
    Mockito
      .`when`(oAuthService.getDestinationOAuthParameterOptional(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull()))
      .thenReturn(Optional.empty())
    assertThrows(
      ResourceNotFoundProblem::class.java,
    ) {
      oauthFlow.getSourceConsentUrl(
        workspaceId,
        definitionId,
        REDIRECT_URL,
        inputOAuthConfiguration,
        getoAuthConfigSpecification(),
        null,
      )
    }
    assertThrows(
      ResourceNotFoundProblem::class.java,
    ) {
      oauthFlow.getDestinationConsentUrl(
        workspaceId,
        definitionId,
        REDIRECT_URL,
        inputOAuthConfiguration,
        getoAuthConfigSpecification(),
        null,
      )
    }
  }

  @Test
  fun testGetConsentUrlIncompleteOAuthParameters() {
    val sourceOAuthParameter =
      SourceOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withSourceDefinitionId(definitionId)
        .withConfiguration(Jsons.emptyObject())
    Mockito
      .`when`(oAuthService.getSourceOAuthParameterOptional(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull()))
      .thenReturn(Optional.of(sourceOAuthParameter))
    val destinationOAuthParameter =
      DestinationOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withDestinationDefinitionId(definitionId)
        .withConfiguration(Jsons.emptyObject())
    Mockito
      .`when`(oAuthService.getDestinationOAuthParameterOptional(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull()))
      .thenReturn(Optional.of(destinationOAuthParameter))
    assertThrows(
      IllegalArgumentException::class.java,
    ) {
      oauthFlow.getSourceConsentUrl(
        workspaceId,
        definitionId,
        REDIRECT_URL,
        inputOAuthConfiguration,
        getoAuthConfigSpecification(),
        sourceOAuthParameter.configuration,
      )
    }
    assertThrows(
      IllegalArgumentException::class.java,
    ) {
      oauthFlow.getDestinationConsentUrl(
        workspaceId,
        definitionId,
        REDIRECT_URL,
        inputOAuthConfiguration,
        getoAuthConfigSpecification(),
        destinationOAuthParameter.configuration,
      )
    }
  }

  @Test
  open fun testGetSourceConsentUrlEmptyOAuthSpec() {
    if (hasDependencyOnConnectorConfigValues()) {
      assertThrows(
        IOException::class.java,
        {
          oauthFlow.getSourceConsentUrl(
            workspaceId,
            definitionId,
            REDIRECT_URL,
            Jsons.emptyObject(),
            null,
            sourceOAuthParameter.configuration,
          )
        },
        "OAuth Flow Implementations with dependencies on connector config can't be supported without OAuthConfigSpecifications",
      )
    } else {
      val consentUrl =
        oauthFlow.getSourceConsentUrl(
          workspaceId,
          definitionId,
          REDIRECT_URL,
          Jsons.emptyObject(),
          null,
          sourceOAuthParameter.configuration,
        )
      assertEquals(expectedConsentUrl, consentUrl)
    }
  }

  @Test
  open fun testGetDestinationConsentUrlEmptyOAuthSpec() {
    if (hasDependencyOnConnectorConfigValues()) {
      assertThrows(
        IOException::class.java,
        {
          oauthFlow.getDestinationConsentUrl(
            workspaceId,
            definitionId,
            REDIRECT_URL,
            Jsons.emptyObject(),
            null,
            destinationOAuthParameter.configuration,
          )
        },
        "OAuth Flow Implementations with dependencies on connector config can't be supported without OAuthConfigSpecifications",
      )
    } else {
      val consentUrl =
        oauthFlow.getDestinationConsentUrl(
          workspaceId,
          definitionId,
          REDIRECT_URL,
          Jsons.emptyObject(),
          null,
          destinationOAuthParameter.configuration,
        )
      assertEquals(expectedConsentUrl, consentUrl)
    }
  }

  @Test
  open fun testGetSourceConsentUrl() {
    val consentUrl =
      oauthFlow.getSourceConsentUrl(
        workspaceId,
        definitionId,
        REDIRECT_URL,
        inputOAuthConfiguration,
        getoAuthConfigSpecification(),
        sourceOAuthParameter.configuration,
      )
    assertEquals(expectedConsentUrl, consentUrl)
  }

  @Test
  open fun testGetDestinationConsentUrl() {
    val consentUrl =
      oauthFlow.getDestinationConsentUrl(
        workspaceId,
        definitionId,
        REDIRECT_URL,
        inputOAuthConfiguration,
        getoAuthConfigSpecification(),
        destinationOAuthParameter.configuration,
      )
    assertEquals(expectedConsentUrl, consentUrl)
  }

  @Test
  fun testCompleteOAuthMissingCode() {
    val queryParams = emptyMap<String, Any>()
    assertThrows(
      IOException::class.java,
    ) {
      oauthFlow.completeSourceOAuth(
        workspaceId,
        definitionId,
        queryParams,
        REDIRECT_URL,
        sourceOAuthParameter.configuration,
      )
    }
  }

  @Test
  open fun testDeprecatedCompleteSourceOAuth() {
    val returnedCredentials = expectedOutput
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(Jsons.serialize(returnedCredentials))
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams

    if (hasDependencyOnConnectorConfigValues()) {
      assertThrows(
        IOException::class.java,
        { oauthFlow.completeSourceOAuth(workspaceId, definitionId, queryParams, REDIRECT_URL, sourceOAuthParameter.configuration) },
        "OAuth Flow Implementations with dependencies on connector config can't be supported in the deprecated APIs",
      )
    } else {
      var actualRawQueryParams: Map<String, Any> =
        oauthFlow.completeSourceOAuth(workspaceId, definitionId, queryParams, REDIRECT_URL, sourceOAuthParameter.configuration)
      for (node in expectedOutputPath) {
        assertNotNull(actualRawQueryParams[node])
        actualRawQueryParams = actualRawQueryParams[node] as Map<String, Any>
      }
      val expectedOutput = returnedCredentials
      val actualQueryParams = actualRawQueryParams
      assertEquals(
        expectedOutput.size,
        actualQueryParams.size,
        String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
      )
      expectedOutput.forEach { (key: String?, value: String?) ->
        assertEquals(
          value,
          actualQueryParams[key],
        )
      }
    }
  }

  @Test
  open fun testDeprecatedCompleteDestinationOAuth() {
    val returnedCredentials = expectedOutput
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(Jsons.serialize(returnedCredentials))
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams

    if (hasDependencyOnConnectorConfigValues()) {
      assertThrows(
        IOException::class.java,
        {
          oauthFlow.completeDestinationOAuth(
            workspaceId,
            definitionId,
            queryParams,
            REDIRECT_URL,
            destinationOAuthParameter.configuration,
          )
        },
        "OAuth Flow Implementations with dependencies on connector config can't be supported in the deprecated APIs",
      )
    } else {
      var actualRawQueryParams: Map<String, Any> =
        oauthFlow.completeDestinationOAuth(
          workspaceId,
          definitionId,
          queryParams,
          REDIRECT_URL,
          destinationOAuthParameter.configuration,
        )
      for (node in expectedOutputPath) {
        assertNotNull(actualRawQueryParams[node])
        actualRawQueryParams = actualRawQueryParams[node] as Map<String, Any>
      }
      val expectedOutput = returnedCredentials
      val actualQueryParams = actualRawQueryParams
      assertEquals(
        expectedOutput.size,
        actualQueryParams.size,
        String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
      )
      expectedOutput.forEach { (key: String?, value: String?) ->
        assertEquals(
          value,
          actualQueryParams[key],
        )
      }
    }
  }

  @Test
  open fun testEmptyOutputCompleteSourceOAuth() {
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(mockedResponse)
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams
    val actualQueryParams =
      oauthFlow.completeSourceOAuth(
        workspaceId,
        definitionId,
        queryParams,
        REDIRECT_URL,
        inputOAuthConfiguration,
        emptyOAuthConfigSpecification,
        sourceOAuthParameter.configuration,
      )
    assertEquals(
      0,
      actualQueryParams.size,
      String.format("Expected no values but got %s", actualQueryParams),
    )
  }

  @Test
  open fun testEmptyOutputCompleteDestinationOAuth() {
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(mockedResponse)
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams
    val actualQueryParams =
      oauthFlow.completeDestinationOAuth(
        workspaceId,
        definitionId,
        queryParams,
        REDIRECT_URL,
        inputOAuthConfiguration,
        emptyOAuthConfigSpecification,
        destinationOAuthParameter.configuration,
      )
    assertEquals(
      0,
      actualQueryParams.size,
      String.format("Expected no values but got %s", actualQueryParams),
    )
  }

  @Test
  open fun testEmptyInputCompleteSourceOAuth() {
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(mockedResponse)
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams
    val actualQueryParams =
      oauthFlow.completeSourceOAuth(
        workspaceId,
        definitionId,
        queryParams,
        REDIRECT_URL,
        Jsons.emptyObject(),
        getoAuthConfigSpecification(),
        sourceOAuthParameter.configuration,
      )
    val expectedOutput = expectedFilteredOutput
    assertEquals(
      expectedOutput.size,
      actualQueryParams.size,
      String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
    )
    expectedOutput.forEach { (key: String?, value: String?) ->
      assertEquals(
        value,
        actualQueryParams[key],
      )
    }
  }

  @Test
  open fun testEmptyInputCompleteDestinationOAuth() {
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(mockedResponse)
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams
    val actualQueryParams =
      oauthFlow.completeDestinationOAuth(
        workspaceId,
        definitionId,
        queryParams,
        REDIRECT_URL,
        Jsons.emptyObject(),
        getoAuthConfigSpecification(),
        destinationOAuthParameter.configuration,
      )
    val expectedOutput = expectedFilteredOutput
    assertEquals(
      expectedOutput.size,
      actualQueryParams.size,
      String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
    )
    expectedOutput.forEach { (key: String?, value: String?) ->
      assertEquals(
        value,
        actualQueryParams[key],
      )
    }
  }

  @Test
  open fun testCompleteSourceOAuth() {
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(mockedResponse)
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams
    val actualQueryParams =
      oauthFlow.completeSourceOAuth(
        workspaceId,
        definitionId,
        queryParams,
        REDIRECT_URL,
        inputOAuthConfiguration,
        getoAuthConfigSpecification(),
        sourceOAuthParameter.configuration,
      )
    val expectedOutput = expectedFilteredOutput
    assertEquals(
      expectedOutput.size,
      actualQueryParams.size,
      String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
    )
    expectedOutput.forEach { (key: String?, value: String?) ->
      assertEquals(
        value,
        actualQueryParams[key],
      )
    }
  }

  @Test
  open fun testCompleteDestinationOAuth() {
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(mockedResponse)
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams
    val actualQueryParams =
      oauthFlow.completeDestinationOAuth(
        workspaceId,
        definitionId,
        queryParams,
        REDIRECT_URL,
        inputOAuthConfiguration,
        getoAuthConfigSpecification(),
        destinationOAuthParameter.configuration,
      )
    val expectedOutput = expectedFilteredOutput
    assertEquals(
      expectedOutput.size,
      actualQueryParams.size,
      String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
    )
    expectedOutput.forEach { (key: String?, value: String?) ->
      assertEquals(
        value,
        actualQueryParams[key],
      )
    }
  }

  @Test
  open fun testValidateOAuthOutputFailure() {
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(mockedResponse)
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams
    val oAuthConfigSpecification = oAuthConfigSpecification
    assertThrows(
      JsonValidationException::class.java,
    ) {
      oauthFlow.completeSourceOAuth(
        workspaceId,
        definitionId,
        queryParams,
        REDIRECT_URL,
        inputOAuthConfiguration,
        oAuthConfigSpecification,
        sourceOAuthParameter.configuration,
      )
    }
    assertThrows(
      JsonValidationException::class.java,
    ) {
      oauthFlow.completeDestinationOAuth(
        workspaceId,
        definitionId,
        queryParams,
        REDIRECT_URL,
        inputOAuthConfiguration,
        oAuthConfigSpecification,
        destinationOAuthParameter.configuration,
      )
    }
  }

  companion object {
    private const val REDIRECT_URL = "https://airbyte.io"
    private const val TYPE = "type"
    private const val TEST_CODE = "test_code"
    private const val STATE = "state"
    private const val EXPECTED_BUT_GOT = "Expected %s values but got\n\t%s\ninstead of\n\t%s"

    internal fun getJsonSchema(properties: Map<String, Any>): JsonNode =
      Jsons.jsonNode(
        mapOf(
          TYPE to "object",
          "additionalProperties" to "false",
          "properties" to properties,
        ),
      )
  }
}
