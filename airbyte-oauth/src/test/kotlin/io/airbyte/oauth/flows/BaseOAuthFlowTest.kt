/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.data.services.OAuthService
import io.airbyte.oauth.BaseOAuthFlow
import io.airbyte.oauth.MoreOAuthParameters
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
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
  @Throws(JsonValidationException::class, IOException::class)
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
      java.util.Map.of(
        REFRESH_TOKEN,
        "refresh_token_response",
        CLIENT_ID,
        MoreOAuthParameters.SECRET_MASK,
        "client_secret",
        MoreOAuthParameters.SECRET_MASK,
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
        java.util.Map.of<String, Any>(
          REFRESH_TOKEN,
          java.util.Map.of(TYPE, "string"),
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
      java.util.Map.of(
        REFRESH_TOKEN,
        "refresh_token_response",
        CLIENT_ID,
        MoreOAuthParameters.SECRET_MASK,
      )

  protected val completeOAuthServerOutputSpecification: JsonNode
        /*
         * @return the output specification used to filter what the oauth flow should be returning
         */
    get() =
      getJsonSchema(
        java.util.Map.of<String, Any>(
          CLIENT_ID,
          java.util.Map.of(TYPE, "string"),
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
    get() = getJsonSchema(java.util.Map.of())

  protected open val oAuthParamConfig: JsonNode?
        /*
         * @return the instance wide config params for this oauth flow
         */
    get() =
      Jsons.jsonNode(
        ImmutableMap
          .builder<Any, Any>()
          .put(CLIENT_ID, "test_client_id")
          .put("client_secret", "test_client_secret")
          .build(),
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
      java.util.Map.of<String, Any>(
        CODE,
        TEST_CODE,
        STATE,
        constantState,
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
            java.util.Map.of<String, Any>(
              CLIENT_ID,
              java.util.Map.of(TYPE, "integer"),
            ),
          ),
        ).withCompleteOauthOutputSpecification(
          getJsonSchema(
            java.util.Map.of<String, Any>(
              REFRESH_TOKEN,
              java.util.Map.of(TYPE, "integer"),
            ),
          ),
        )

  @Test
  fun testGetDefaultOutputPath() {
    Assertions.assertEquals(expectedOutputPath, oauthFlow.getDefaultOAuthOutputPath())
  }

  @Test
  fun testValidateInputOAuthConfigurationFailure() {
    val invalidInputOAuthConfiguration = Jsons.jsonNode(java.util.Map.of("UnexpectedRandomField", 42))
    Assertions.assertThrows(
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
    Assertions.assertThrows(
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
    Assertions.assertThrows(
      JsonValidationException::class.java,
    ) {
      oauthFlow.completeSourceOAuth(
        workspaceId,
        definitionId,
        java.util.Map.of(),
        REDIRECT_URL,
        invalidInputOAuthConfiguration,
        getoAuthConfigSpecification(),
        sourceOAuthParameter.configuration,
      )
    }
    Assertions.assertThrows(
      JsonValidationException::class.java,
    ) {
      oauthFlow.completeDestinationOAuth(
        workspaceId,
        definitionId,
        java.util.Map.of(),
        REDIRECT_URL,
        invalidInputOAuthConfiguration,
        getoAuthConfigSpecification(),
        destinationOAuthParameter.configuration,
      )
    }
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testGetConsentUrlEmptyOAuthParameters() {
    Mockito
      .`when`(
        oAuthService.getSourceOAuthParameterOptional(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull()),
      ).thenReturn(Optional.empty())
    Mockito
      .`when`(oAuthService.getDestinationOAuthParameterOptional(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull()))
      .thenReturn(Optional.empty())
    Assertions.assertThrows(
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
    Assertions.assertThrows(
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
  @Throws(IOException::class, JsonValidationException::class)
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
    Assertions.assertThrows(
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
    Assertions.assertThrows(
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
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
  open fun testGetSourceConsentUrlEmptyOAuthSpec() {
    if (hasDependencyOnConnectorConfigValues()) {
      Assertions.assertThrows(
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
      Assertions.assertEquals(expectedConsentUrl, consentUrl)
    }
  }

  @Test
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
  open fun testGetDestinationConsentUrlEmptyOAuthSpec() {
    if (hasDependencyOnConnectorConfigValues()) {
      Assertions.assertThrows(
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
      Assertions.assertEquals(expectedConsentUrl, consentUrl)
    }
  }

  @Test
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
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
    Assertions.assertEquals(expectedConsentUrl, consentUrl)
  }

  @Test
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
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
    Assertions.assertEquals(expectedConsentUrl, consentUrl)
  }

  @Test
  fun testCompleteOAuthMissingCode() {
    val queryParams = java.util.Map.of<String, Any>()
    Assertions.assertThrows(
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
  @Throws(IOException::class, InterruptedException::class, ConfigNotFoundException::class)
  open fun testDeprecatedCompleteSourceOAuth() {
    val returnedCredentials = expectedOutput
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(Jsons.serialize(returnedCredentials))
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams

    if (hasDependencyOnConnectorConfigValues()) {
      Assertions.assertThrows(
        IOException::class.java,
        { oauthFlow.completeSourceOAuth(workspaceId, definitionId, queryParams, REDIRECT_URL, sourceOAuthParameter.configuration) },
        "OAuth Flow Implementations with dependencies on connector config can't be supported in the deprecated APIs",
      )
    } else {
      var actualRawQueryParams: Map<String, Any> =
        oauthFlow.completeSourceOAuth(workspaceId, definitionId, queryParams, REDIRECT_URL, sourceOAuthParameter.configuration)
      for (node in expectedOutputPath) {
        Assertions.assertNotNull(actualRawQueryParams[node])
        actualRawQueryParams = actualRawQueryParams[node] as Map<String, Any>
      }
      val expectedOutput = returnedCredentials
      val actualQueryParams = actualRawQueryParams
      Assertions.assertEquals(
        expectedOutput.size,
        actualQueryParams.size,
        String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
      )
      expectedOutput.forEach { (key: String?, value: String?) ->
        Assertions.assertEquals(
          value,
          actualQueryParams[key],
        )
      }
    }
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class, InterruptedException::class)
  open fun testDeprecatedCompleteDestinationOAuth() {
    val returnedCredentials = expectedOutput
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(Jsons.serialize(returnedCredentials))
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams

    if (hasDependencyOnConnectorConfigValues()) {
      Assertions.assertThrows(
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
        Assertions.assertNotNull(actualRawQueryParams[node])
        actualRawQueryParams = actualRawQueryParams[node] as Map<String, Any>
      }
      val expectedOutput = returnedCredentials
      val actualQueryParams = actualRawQueryParams
      Assertions.assertEquals(
        expectedOutput.size,
        actualQueryParams.size,
        String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
      )
      expectedOutput.forEach { (key: String?, value: String?) ->
        Assertions.assertEquals(
          value,
          actualQueryParams[key],
        )
      }
    }
  }

  @Test
  @Throws(
    IOException::class,
    InterruptedException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
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
    Assertions.assertEquals(
      0,
      actualQueryParams.size,
      String.format("Expected no values but got %s", actualQueryParams),
    )
  }

  @Test
  @Throws(
    IOException::class,
    InterruptedException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
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
    Assertions.assertEquals(
      0,
      actualQueryParams.size,
      String.format("Expected no values but got %s", actualQueryParams),
    )
  }

  @Test
  @Throws(
    IOException::class,
    InterruptedException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
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
    Assertions.assertEquals(
      expectedOutput.size,
      actualQueryParams.size,
      String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
    )
    expectedOutput.forEach { (key: String?, value: String?) ->
      Assertions.assertEquals(
        value,
        actualQueryParams[key],
      )
    }
  }

  @Test
  @Throws(
    IOException::class,
    InterruptedException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
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
    Assertions.assertEquals(
      expectedOutput.size,
      actualQueryParams.size,
      String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
    )
    expectedOutput.forEach { (key: String?, value: String?) ->
      Assertions.assertEquals(
        value,
        actualQueryParams[key],
      )
    }
  }

  @Test
  @Throws(
    IOException::class,
    InterruptedException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
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
    Assertions.assertEquals(
      expectedOutput.size,
      actualQueryParams.size,
      String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
    )
    expectedOutput.forEach { (key: String?, value: String?) ->
      Assertions.assertEquals(
        value,
        actualQueryParams[key],
      )
    }
  }

  @Test
  @Throws(
    IOException::class,
    InterruptedException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
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
    Assertions.assertEquals(
      expectedOutput.size,
      actualQueryParams.size,
      String.format(EXPECTED_BUT_GOT, expectedOutput.size, actualQueryParams, expectedOutput),
    )
    expectedOutput.forEach { (key: String?, value: String?) ->
      Assertions.assertEquals(
        value,
        actualQueryParams[key],
      )
    }
  }

  @Test
  @Throws(
    IOException::class,
    InterruptedException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
  )
  open fun testValidateOAuthOutputFailure() {
    val response = Mockito.mock(HttpResponse::class.java)
    Mockito.`when`(response.body()).thenReturn(mockedResponse)
    Mockito.`when`(httpClient.send(ArgumentMatchers.any(), ArgumentMatchers.any<HttpResponse.BodyHandler<*>>())).thenReturn(response)
    val queryParams = queryParams
    val oAuthConfigSpecification = oAuthConfigSpecification
    Assertions.assertThrows(
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
    Assertions.assertThrows(
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
    private const val REFRESH_TOKEN = "refresh_token"
    private const val CLIENT_ID = "client_id"
    private const val TYPE = "type"
    private const val CODE = "code"
    private const val TEST_CODE = "test_code"
    private const val STATE = "state"
    private const val EXPECTED_BUT_GOT = "Expected %s values but got\n\t%s\ninstead of\n\t%s"

    internal fun getJsonSchema(properties: Map<String, Any>): JsonNode =
      Jsons.jsonNode(
        java.util.Map.of(
          TYPE,
          "object",
          "additionalProperties",
          "false",
          "properties",
          properties,
        ),
      )
  }
}
