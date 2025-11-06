/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.analytics.TrackingClient
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ScopeType
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.oauth.MoreOAuthParameters
import io.airbyte.protocol.models.v0.AdvancedAuth
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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
  private lateinit var secretReferenceService: SecretReferenceService

  @BeforeEach
  fun setup() {
    trackingClient = mockk<TrackingClient>(relaxed = true)
    sourceService = mockk<SourceService>()
    destinationService = mockk<DestinationService>()
    actorDefinitionVersionHelper = mockk<ActorDefinitionVersionHelper>()
    oAuthService = mockk<OAuthService>()
    secretReferenceService = mockk<SecretReferenceService>()
    oAuthConfigSupplier =
      OAuthConfigSupplier(trackingClient, actorDefinitionVersionHelper, oAuthService, sourceService, destinationService, secretReferenceService)
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
  fun testNoOAuthInjectionBecauseEmptyParams() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone(config))
    Assertions.assertEquals(config, actualConfig)
    assertNoTracking()
  }

  @Test
  fun testNoAuthMaskingBecauseEmptyParams() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val actualConfig =
      oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, clone(config), testConnectorSpecification)
    Assertions.assertEquals(config, actualConfig)
  }

  @Test
  fun testNoOAuthInjectionBecauseMissingPredicateKey() {
    setupStandardDefinitionMock(
      createAdvancedAuth()!!
        .withPredicateKey(listOf<String?>("some_random_fields", AUTH_TYPE))
        .withPredicateValue(OAUTH),
    )
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    setupOAuthParamMocks(generateOAuthParameters(), workspaceId)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone(config))
    Assertions.assertEquals(config, actualConfig)
    assertNoTracking()
  }

  @Test
  fun testNoOAuthInjectionBecauseWrongPredicateValue() {
    setupStandardDefinitionMock(
      createAdvancedAuth()!!
        .withPredicateKey(listOf<String?>(CREDENTIALS, AUTH_TYPE))
        .withPredicateValue("wrong_auth_type"),
    )
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    setupOAuthParamMocks(generateOAuthParameters(), workspaceId)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone(config))
    Assertions.assertEquals(config, actualConfig)
    assertNoTracking()
  }

  @Test
  fun testNoOAuthMaskingBecauseWrongPredicateValue() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val spec: ConnectorSpecification? =
      createConnectorSpecification(
        createAdvancedAuth()!!
          .withPredicateKey(listOf<String?>(CREDENTIALS, AUTH_TYPE))
          .withPredicateValue("wrong_auth_type"),
      )
    setupOAuthParamMocks(generateOAuthParameters(), workspaceId)
    val actualConfig = oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, clone(config), spec)
    Assertions.assertEquals(config, actualConfig)
  }

  @Test
  fun testOAuthInjection() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters, workspaceId)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone(config))
    val expectedConfig: JsonNode =
      getExpectedNode((oauthParameters[API_CLIENT] as String?)!!)
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  fun testOAuthMasking() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters, workspaceId)
    val actualConfig =
      oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, clone(config), testConnectorSpecification)
    val expectedConfig: JsonNode = getExpectedNode(MoreOAuthParameters.SECRET_MASK)
    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  @Test
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
    setupOAuthParamMocks(oauthParameters, workspaceId)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone(config))
    val expectedConfig: JsonNode =
      getExpectedNode((oauthParameters[API_CLIENT] as String?)!!)
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
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
    setupOAuthParamMocks(oauthParameters, workspaceId)
    val actualConfig = oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, clone(config), spec)
    val expectedConfig: JsonNode = getExpectedNode(MoreOAuthParameters.SECRET_MASK)
    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  @Test
  fun testOAuthInjectionWithoutPredicateValue() {
    setupStandardDefinitionMock(
      createAdvancedAuth()!!
        .withPredicateKey(listOf<String?>(CREDENTIALS, AUTH_TYPE))
        .withPredicateValue(""),
    )
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters, workspaceId)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone(config))
    val expectedConfig: JsonNode =
      getExpectedNode((oauthParameters[API_CLIENT] as String?)!!)
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  fun testOAuthMaskingWithoutPredicateValue() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val spec: ConnectorSpecification? =
      createConnectorSpecification(
        createAdvancedAuth()!!
          .withPredicateKey(listOf<String?>(CREDENTIALS, AUTH_TYPE))
          .withPredicateValue(""),
      )
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters, workspaceId)
    val actualConfig = oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, clone(config), spec)
    val expectedConfig: JsonNode = getExpectedNode(MoreOAuthParameters.SECRET_MASK)
    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  @Test
  fun testOAuthFullInjectionBecauseNoOAuthSpec() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        any<StandardSourceDefinition>(),
        workspaceId,
        sourceId,
      )
    } returns testSourceVersion.withSpec(null)
    setupOAuthParamMocks(oauthParameters, workspaceId)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone(config))
    val expectedConfig = (clone(config) as ObjectNode)
    for (key in oauthParameters.keys) {
      expectedConfig.set<JsonNode?>(key, jsonNode<Any?>(oauthParameters[key]))
    }
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  fun testOAuthNoMaskingBecauseNoOAuthSpec() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    setupOAuthParamMocks(oauthParameters, workspaceId)
    val actualConfig = oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, clone(config), null)
    Assertions.assertEquals(config, actualConfig)
  }

  @Test
  fun testOAuthInjectionScopedToWorkspace() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateOAuthParameters()
    val oauthParameterId = UUID.randomUUID()
    val configuration = jsonNode<MutableMap<String?, Any?>?>(oauthParameters)
    every {
      oAuthService.getSourceOAuthParameterOptional(
        any<UUID>(),
        any<UUID>(),
      )
    } returns
      Optional.of(
        SourceOAuthParameter()
          .withOauthParameterId(oauthParameterId)
          .withSourceDefinitionId(sourceDefinitionId)
          .withWorkspaceId(workspaceId)
          .withConfiguration(configuration),
      )
    every {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        any(),
        any(),
        any(),
      )
    } returns
      ConfigWithSecretReferences(
        originalConfig = configuration,
        referencedSecrets = emptyMap(),
      )
    every {
      secretReferenceService.getHydratedConfiguration(
        any(),
        any(),
        any(),
        any(),
      )
    } returns configuration
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone(config))
    val expectedConfig: JsonNode =
      getExpectedNode((oauthParameters[API_CLIENT] as String?)!!)
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  fun testOAuthFullInjectionBecauseNoOAuthSpecNestedParameters() {
    // Until https://github.com/airbytehq/airbyte/issues/7624 is solved, we need to handle nested oauth
    // parameters
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateNestedOAuthParameters()
    setupOAuthParamMocks(oauthParameters, workspaceId)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone(config))
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
            (oauthParameters[CREDENTIALS] as MutableMap<String?, String?>)[API_CLIENT],
          ),
        ),
      )
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  fun testOAuthInjectionNestedParameters() {
    // Until https://github.com/airbytehq/airbyte/issues/7624 is solved, we need to handle nested oauth
    // parameters
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateNestedOAuthParameters()
    setupOAuthParamMocks(oauthParameters, workspaceId)
    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone(config))
    val expectedConfig: JsonNode =
      getExpectedNode(
        (
          (oauthParameters[CREDENTIALS] as MutableMap<String?, Any?>)[API_CLIENT] as String?
        )!!,
      )
    Assertions.assertEquals(expectedConfig, actualConfig)
    assertTracking(workspaceId)
  }

  @Test
  fun testOAuthMaskingNestedParameters() {
    // Until https://github.com/airbytehq/airbyte/issues/7624 is solved, we need to handle nested oauth
    // parameters
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val oauthParameters: MutableMap<String?, Any?> = generateNestedOAuthParameters()
    setupOAuthParamMocks(oauthParameters, workspaceId)
    val actualConfig =
      oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, clone(config), testConnectorSpecification)
    val expectedConfig: JsonNode = getExpectedNode(MoreOAuthParameters.SECRET_MASK)
    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  @Test
  fun testOAuthInjectingNestedSecrets() {
    val config: JsonNode = generateJsonConfig()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val oauthParameters = Map.of<String?, Any?>(CREDENTIALS, generateSecretOAuthParameters())
    setupOAuthParamMocks(oauthParameters, workspaceId)

    val actualConfig = oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, sourceId, workspaceId, clone(config))
    val expectedConfig: JsonNode = getExpectedNode(secretCoordinateMap())
    Assertions.assertEquals(expectedConfig, actualConfig)
  }

  private fun setupStandardDefinitionMock(advancedAuth: AdvancedAuth?) {
    every { sourceService.getStandardSourceDefinition(any<UUID>()) } returns testSourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        any<StandardSourceDefinition>(),
        any<UUID>(),
        any<UUID>(),
      )
    } returns testSourceVersion.withSpec(createConnectorSpecification(advancedAuth))

    every {
      oAuthService.getSourceOAuthParameterOptional(any<UUID>(), any<UUID>())
    } returns Optional.empty()
  }

  private fun setupOAuthParamMocks(
    oauthParameters: MutableMap<String?, Any?>?,
    workspaceId: UUID,
  ): UUID {
    val oauthParameterId = UUID.randomUUID()
    val configuration = jsonNode<MutableMap<String?, Any?>?>(oauthParameters)
    every {
      oAuthService.getSourceOAuthParameterOptional(
        workspaceId,
        any<UUID>(),
      )
    } returns
      Optional.of(
        SourceOAuthParameter()
          .withOauthParameterId(oauthParameterId)
          .withSourceDefinitionId(sourceDefinitionId)
          .withWorkspaceId(workspaceId)
          .withConfiguration(configuration),
      )
    every {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        any(),
        any(),
        any(),
      )
    } returns
      ConfigWithSecretReferences(
        originalConfig = configuration,
        referencedSecrets = emptyMap(),
      )
    every {
      secretReferenceService.getHydratedConfiguration(
        any(),
        any(),
        any(),
        any(),
      )
    } returns configuration
    return oauthParameterId
  }

  private fun assertNoTracking() {
    verify(exactly = 0) {
      trackingClient.track(
        any<UUID>(),
        any<ScopeType>(),
        any<String>(),
        any<kotlin.collections.Map<String, Any?>>(),
      )
    }
  }

  private fun assertTracking(workspaceId: UUID) {
    verify(exactly = 1) {
      trackingClient.track(
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
              jsonNode(
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
