/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.analytics.TrackingClient
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ScopeType
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SourceService
import io.airbyte.oauth.MoreOAuthParameters
import io.airbyte.protocol.models.v0.AdvancedAuth
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.List
import java.util.Map
import java.util.Optional
import java.util.UUID

internal class OAuthConfigSupplierTest {
  private lateinit var trackingClient: TrackingClient
  private lateinit var oAuthConfigSupplier: OAuthConfigSupplier
  private lateinit var sourceDefinitionId: UUID
  private lateinit var testSourceDefinition: StandardSourceDefinition
  private lateinit var testSourceVersion: ActorDefinitionVersion
  private lateinit var testConnectorSpecification: ConnectorSpecification
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var oAuthService: OAuthService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService

  @BeforeEach
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun setup() {
    trackingClient = mock<TrackingClient>()
    sourceService = mock<SourceService>()
    destinationService = mock<DestinationService>()
    actorDefinitionVersionHelper = mock<ActorDefinitionVersionHelper>()
    oAuthService = mock<OAuthService>()
    oAuthConfigSupplier =
      OAuthConfigSupplier(trackingClient, actorDefinitionVersionHelper, oAuthService, sourceService, destinationService)
    sourceDefinitionId = UUID.randomUUID()
    testSourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(sourceDefinitionId)
        .withName("test")
    testSourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository("test/test")
        .withDockerImageTag("dev")
        .withSpec(null)

    val advancedAuth: AdvancedAuth =
      createAdvancedAuth()!!
        .withPredicateKey(listOf(CREDENTIALS, AUTH_TYPE))
        .withPredicateValue(OAUTH)

    testConnectorSpecification = createConnectorSpecification(advancedAuth)!!

    setupStandardDefinitionMock(advancedAuth)
  }

  @Test
  @Throws(IOException::class)
  fun testNoOAuthInjectionBecauseEmptyParams() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone<JsonNode>(config))
    Assertions.assertEquals(config, actualConfig)
    assertNoTracking()
  }

  @Test
  @Throws(IOException::class)
  fun testNoAuthMaskingBecauseEmptyParams() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val actualConfig =
      oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, clone<JsonNode>(config), testConnectorSpecification)
    Assertions.assertEquals(config, actualConfig)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testNoOAuthInjectionBecauseMissingPredicateKey() {
    setupStandardDefinitionMock(
      createAdvancedAuth()!!
        .withPredicateKey(List.of<String?>("some_random_fields", AUTH_TYPE))
        .withPredicateValue(OAUTH),
    )
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    setupOAuthParamMocks(generateOAuthParameters())
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone<JsonNode>(config))
    Assertions.assertEquals(config, actualConfig)
    assertNoTracking()
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testNoOAuthInjectionBecauseWrongPredicateValue() {
    setupStandardDefinitionMock(
      createAdvancedAuth()!!
        .withPredicateKey(List.of<String?>(CREDENTIALS, AUTH_TYPE))
        .withPredicateValue("wrong_auth_type"),
    )
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    setupOAuthParamMocks(generateOAuthParameters())
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone<JsonNode>(config))
    Assertions.assertEquals(config, actualConfig)
    assertNoTracking()
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun testNoOAuthMaskingBecauseWrongPredicateValue() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val spec: ConnectorSpecification? =
      createConnectorSpecification(
        createAdvancedAuth()!!
          .withPredicateKey(List.of<String?>(CREDENTIALS, AUTH_TYPE))
          .withPredicateValue("wrong_auth_type"),
      )
    setupOAuthParamMocks(generateOAuthParameters())
    val actualConfig = oAuthConfigSupplier!!.maskSourceOAuthParameters(sourceDefinitionId!!, workspaceId, clone<JsonNode>(config), spec)
    Assertions.assertEquals(config, actualConfig)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testOAuthInjection() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone<JsonNode>(config))
    val expectedConfig: JsonNode =
      Companion.getExpectedNode((oauthParameters.get(OAuthConfigSupplierTest.Companion.API_CLIENT) as kotlin.String?)!!)
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testOAuthMasking() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters)
    val actualConfig =
      oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, clone<JsonNode>(config), testConnectorSpecification)
    val expectedConfig: JsonNode = getExpectedNode(MoreOAuthParameters.SECRET_MASK)
    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testOAuthInjectionWithoutPredicate() {
    setupStandardDefinitionMock(
      createAdvancedAuth()!!
        .withPredicateKey(null)
        .withPredicateValue(null),
    )
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone<JsonNode>(config))
    val expectedConfig: JsonNode =
      Companion.getExpectedNode((oauthParameters.get(OAuthConfigSupplierTest.Companion.API_CLIENT) as kotlin.String?)!!)
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testOAuthMaskingWithoutPredicate() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val spec: ConnectorSpecification? =
      createConnectorSpecification(
        createAdvancedAuth()!!
          .withPredicateKey(null)
          .withPredicateValue(null),
      )
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters)
    val actualConfig = oAuthConfigSupplier!!.maskSourceOAuthParameters(sourceDefinitionId!!, workspaceId, clone<JsonNode>(config), spec)
    val expectedConfig: JsonNode = getExpectedNode(MoreOAuthParameters.SECRET_MASK)
    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testOAuthInjectionWithoutPredicateValue() {
    setupStandardDefinitionMock(
      createAdvancedAuth()!!
        .withPredicateKey(List.of<String?>(CREDENTIALS, AUTH_TYPE))
        .withPredicateValue(""),
    )
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone<JsonNode>(config))
    val expectedConfig: JsonNode =
      Companion.getExpectedNode((oauthParameters.get(OAuthConfigSupplierTest.Companion.API_CLIENT) as kotlin.String?)!!)
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testOAuthMaskingWithoutPredicateValue() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val spec: ConnectorSpecification? =
      createConnectorSpecification(
        createAdvancedAuth()!!
          .withPredicateKey(List.of<String?>(CREDENTIALS, AUTH_TYPE))
          .withPredicateValue(""),
      )
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters)
    val actualConfig = oAuthConfigSupplier!!.maskSourceOAuthParameters(sourceDefinitionId!!, workspaceId, clone<JsonNode>(config), spec)
    val expectedConfig: JsonNode = getExpectedNode(MoreOAuthParameters.SECRET_MASK)
    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testOAuthFullInjectionBecauseNoOAuthSpec() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    whenever(
      actorDefinitionVersionHelper!!.getSourceVersion(
        any<StandardSourceDefinition>(),
        eq(workspaceId),
        eq(sourceId),
      ),
    ).thenReturn(
      testSourceVersion!!.withSpec(null),
    )
    setupOAuthParamMocks(oauthParameters)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone<JsonNode>(config))
    val expectedConfig = (clone<JsonNode>(config) as ObjectNode)
    for (key in oauthParameters.keys) {
      expectedConfig.set<JsonNode?>(key, jsonNode<Any?>(oauthParameters.get(key)))
    }
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testOAuthNoMaskingBecauseNoOAuthSpec() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters)
    val actualConfig = oAuthConfigSupplier!!.maskSourceOAuthParameters(sourceDefinitionId!!, workspaceId, clone<JsonNode>(config), null)
    Assertions.assertEquals(config, actualConfig)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testOAuthInjectionScopedToWorkspace() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    whenever(
      oAuthService.getSourceOAuthParameterOptional(
        any<UUID>(),
        any<UUID>(),
      ),
    ).thenReturn(
      Optional.of<SourceOAuthParameter>(
        SourceOAuthParameter()
          .withOauthParameterId(UUID.randomUUID())
          .withSourceDefinitionId(sourceDefinitionId)
          .withWorkspaceId(workspaceId)
          .withConfiguration(jsonNode<MutableMap<String?, Any?>?>(oauthParameters)),
      ),
    )
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone<JsonNode>(config))
    val expectedConfig: JsonNode =
      Companion.getExpectedNode((oauthParameters.get(OAuthConfigSupplierTest.Companion.API_CLIENT) as kotlin.String?)!!)
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testOAuthFullInjectionBecauseNoOAuthSpecNestedParameters() {
    // Until https://github.com/airbytehq/airbyte/issues/7624 is solved, we need to handle nested oauth
    // parameters
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateNestedOAuthParameters()
    setupOAuthParamMocks(oauthParameters)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone<JsonNode>(config))
    val expectedConfig =
      jsonNode<MutableMap<String, Any>?>(
        Map.of<String?, Any?>(
          EXISTING_FIELD_NAME,
          EXISTING_FIELD_VALUE,
          CREDENTIALS,
          Map.of<String?, String?>(
            API_SECRET,
            SECRET_TWO,
            AUTH_TYPE,
            OAUTH,
            API_CLIENT,
            (oauthParameters.get(CREDENTIALS) as MutableMap<String?, String?>).get(API_CLIENT),
          ),
        ),
      )
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testOAuthInjectionNestedParameters() {
    // Until https://github.com/airbytehq/airbyte/issues/7624 is solved, we need to handle nested oauth
    // parameters
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateNestedOAuthParameters()
    setupOAuthParamMocks(oauthParameters)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone<JsonNode>(config))
    val expectedConfig: JsonNode =
      Companion.getExpectedNode(
        (
          (oauthParameters.get(OAuthConfigSupplierTest.Companion.CREDENTIALS) as kotlin.collections.MutableMap<kotlin.String?, kotlin.Any?>).get(
            OAuthConfigSupplierTest.Companion.API_CLIENT,
          ) as kotlin.String?
        )!!,
      )
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testOAuthMaskingNestedParameters() {
    // Until https://github.com/airbytehq/airbyte/issues/7624 is solved, we need to handle nested oauth
    // parameters
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateNestedOAuthParameters()
    setupOAuthParamMocks(oauthParameters)
    val actualConfig =
      oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, clone<JsonNode>(config), testConnectorSpecification)
    val expectedConfig: JsonNode = getExpectedNode(MoreOAuthParameters.SECRET_MASK)
    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testOAuthInjectingNestedSecrets() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters = Map.of<String?, Any?>(CREDENTIALS, generateSecretOAuthParameters())
    setupOAuthParamMocks(oauthParameters)

    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone<JsonNode>(config))
    val expectedConfig: JsonNode = getExpectedNode(secretCoordinateMap())
    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  private fun setupStandardDefinitionMock(advancedAuth: AdvancedAuth?) {
    whenever(sourceService.getStandardSourceDefinition(any<UUID>()))
      .thenReturn(testSourceDefinition)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        any<StandardSourceDefinition>(),
        any<UUID>(),
        any<UUID>(),
      ),
    ).thenReturn(
      testSourceVersion
        .withSpec(createConnectorSpecification(advancedAuth)),
    )
  }

  @Throws(JsonValidationException::class, IOException::class)
  private fun setupOAuthParamMocks(oauthParameters: MutableMap<String?, Any?>?) {
    whenever(
      oAuthService.getSourceOAuthParameterOptional(
        any<UUID>(),
        any<UUID>(),
      ),
    ).thenReturn(
      Optional.of<SourceOAuthParameter>(
        SourceOAuthParameter()
          .withOauthParameterId(UUID.randomUUID())
          .withSourceDefinitionId(sourceDefinitionId)
          .withWorkspaceId(null)
          .withConfiguration(jsonNode<MutableMap<String?, Any?>?>(oauthParameters)),
      ),
    )
  }

  private fun assertNoTracking() {
    verify(trackingClient, times(0)).track(
      any<UUID>(),
      any<ScopeType>(),
      any<String>(),
      any<kotlin.collections.Map<String, Any?>>(),
    )
  }

  private fun assertTracking(workspaceId: UUID) {
    verify(trackingClient, times(1)).track(
      workspaceId,
      ScopeType.WORKSPACE,
      "OAuth Injection - Backend",
      mapOf(
        "connector_source" to "test",
        "connector_source_definition_id" to sourceDefinitionId,
        "connector_source_docker_repository" to "test/test",
        "connector_source_version" to "dev",
      ),
    )
  }

  companion object {
    const val API_CLIENT: String = "api_client"
    const val CREDENTIALS: String = "credentials"
    const val PROPERTIES: String = "properties"

    private const val AUTH_TYPE = "auth_type"
    private const val OAUTH = "oauth"
    private const val API_SECRET = "api_secret"

    // Existing field k/v used to test that we don't overwrite/lose unrelated data during injection
    private const val EXISTING_FIELD_NAME = "fieldName"
    private const val EXISTING_FIELD_VALUE = "fieldValue"

    private const val SECRET_ONE = "mysecret"
    private const val SECRET_TWO = "123"

    private fun createConnectorSpecification(advancedAuth: AdvancedAuth?): ConnectorSpecification? =
      ConnectorSpecification().withAdvancedAuth(advancedAuth)

    private fun createAdvancedAuth(): AdvancedAuth? =
      AdvancedAuth()
        .withAuthFlowType(AdvancedAuth.AuthFlowType.OAUTH_2_0)
        .withOauthConfigSpecification(
          OAuthConfigSpecification()
            .withCompleteOauthServerOutputSpecification(
              Jsons.jsonNode(
                mapOf(
                  PROPERTIES to
                    mapOf(
                      API_CLIENT to
                        mapOf(
                          "type" to "string",
                          OAuthConfigSupplier.PATH_IN_CONNECTOR_CONFIG to listOf(CREDENTIALS, API_CLIENT),
                        ),
                    ),
                ),
              ),
            ),
        )

    private fun generateJsonConfig(): ObjectNode =
      jsonNode<MutableMap<String, Any>?>(
        Map.of<String?, Any?>(
          EXISTING_FIELD_NAME,
          EXISTING_FIELD_VALUE,
          CREDENTIALS,
          Map.of<String?, String?>(
            API_SECRET,
            SECRET_TWO,
            AUTH_TYPE,
            OAUTH,
          ),
        ),
      ) as ObjectNode

    private fun generateOAuthParameters(): MutableMap<String?, Any?> =
      Map.of<String?, Any?>(
        API_SECRET,
        SECRET_ONE,
        API_CLIENT,
        UUID.randomUUID().toString(),
      )

    private fun generateSecretOAuthParameters(): MutableMap<String?, Any?> =
      Map.of<String?, Any?>(
        API_SECRET,
        SECRET_ONE,
        API_CLIENT,
        secretCoordinateMap(),
      )

    private fun secretCoordinateMap(): MutableMap<String?, Any?> = Map.of<String?, Any?>("_secret", "secret_coordinate")

    private fun generateNestedOAuthParameters(): MutableMap<String?, Any?> = Map.of<String?, Any?>(CREDENTIALS, generateOAuthParameters())

    private fun getExpectedNode(apiClient: String): JsonNode =
      jsonNode<MutableMap<String, Any>?>(
        Map.of<String?, Any?>(
          EXISTING_FIELD_NAME,
          EXISTING_FIELD_VALUE,
          CREDENTIALS,
          Map.of<String?, String?>(
            API_SECRET,
            SECRET_TWO,
            AUTH_TYPE,
            OAUTH,
            API_CLIENT,
            apiClient,
          ),
        ),
      )

    private fun getExpectedNode(apiClient: MutableMap<String?, Any?>): JsonNode =
      jsonNode<MutableMap<String, Any>?>(
        Map.of<String?, Any?>(
          EXISTING_FIELD_NAME,
          EXISTING_FIELD_VALUE,
          CREDENTIALS,
          Map.of<String?, Any?>(
            API_SECRET,
            SECRET_TWO,
            AUTH_TYPE,
            OAUTH,
            API_CLIENT,
            apiClient,
          ),
        ),
      )
  }
}
