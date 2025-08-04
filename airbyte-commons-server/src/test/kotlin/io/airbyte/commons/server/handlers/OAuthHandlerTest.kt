/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.analytics.TrackingClient
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.CompleteSourceOauthRequest
import io.airbyte.api.model.generated.SetInstancewideDestinationOauthParamsRequestBody
import io.airbyte.api.model.generated.SetInstancewideSourceOauthParamsRequestBody
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.handlers.helpers.OAuthHelper.mapToCompleteOAuthResponse
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.oauth.OAuthImplementationFactory
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import java.io.IOException
import java.util.Map
import java.util.Optional
import java.util.UUID

internal class OAuthHandlerTest {
  private lateinit var handler: OAuthHandler
  private lateinit var trackingClient: TrackingClient
  private lateinit var oauthImplementationFactory: OAuthImplementationFactory
  private lateinit var secretsRepositoryReader: SecretsRepositoryReader
  private lateinit var secretsRepositoryWriter: SecretsRepositoryWriter
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var oauthService: OAuthService
  private lateinit var secretPersistenceService: SecretPersistenceService
  private lateinit var secretPersistenceConfigService: SecretPersistenceConfigService
  private lateinit var secretReferenceService: SecretReferenceService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var metricClient: MetricClient
  private lateinit var featureFlagClient: FeatureFlagClient

  @BeforeEach
  fun init() {
    metricClient = mock<MetricClient>()
    trackingClient = mock<TrackingClient>()
    oauthImplementationFactory = mock<OAuthImplementationFactory>()
    secretsRepositoryReader = mock<SecretsRepositoryReader>()
    secretsRepositoryWriter = mock<SecretsRepositoryWriter>()
    actorDefinitionVersionHelper = mock<ActorDefinitionVersionHelper>()
    featureFlagClient = mock<TestClient>()
    sourceService = mock<SourceService>()
    destinationService = mock<DestinationService>()
    oauthService = mock<OAuthService>()
    secretPersistenceConfigService = mock<SecretPersistenceConfigService>()
    secretPersistenceService = mock<SecretPersistenceService>()
    secretReferenceService = mock<SecretReferenceService>()
    workspaceService = mock<WorkspaceService>()
    handler =
      OAuthHandler(
        oauthImplementationFactory,
        trackingClient,
        secretsRepositoryWriter,
        secretsRepositoryReader,
        actorDefinitionVersionHelper,
        featureFlagClient,
        sourceService,
        destinationService,
        oauthService,
        secretPersistenceConfigService,
        secretPersistenceService,
        secretReferenceService,
        workspaceService,
        metricClient,
      )
  }

  @Test
  @Throws(IOException::class)
  fun setSourceInstancewideOauthParams() {
    val sourceDefId = UUID.randomUUID()
    val params: MutableMap<String?, Any?> = HashMap<String?, Any?>()
    params.put(CLIENT_ID_KEY, CLIENT_ID)
    params.put(CLIENT_SECRET_KEY, CLIENT_SECRET)

    val actualRequest =
      SetInstancewideSourceOauthParamsRequestBody()
        .sourceDefinitionId(sourceDefId)
        .params(params)

    handler.setSourceInstancewideOauthParams(actualRequest)

    val argument = argumentCaptor<SourceOAuthParameter>()
    Mockito.verify<OAuthService?>(oauthService).writeSourceOAuthParam(argument.capture())
    Assertions.assertEquals(jsonNode<MutableMap<String?, Any?>?>(params), argument.firstValue.configuration)
    Assertions.assertEquals(sourceDefId, argument.firstValue.sourceDefinitionId)
  }

  @Test
  @Throws(IOException::class)
  fun resetSourceInstancewideOauthParams() {
    val sourceDefId = UUID.randomUUID()
    val firstParams: MutableMap<String?, Any?> = HashMap<String?, Any?>()
    firstParams.put(CLIENT_ID_KEY, CLIENT_ID)
    firstParams.put(CLIENT_SECRET_KEY, CLIENT_SECRET)
    val firstRequest =
      SetInstancewideSourceOauthParamsRequestBody()
        .sourceDefinitionId(sourceDefId)
        .params(firstParams)
    handler.setSourceInstancewideOauthParams(firstRequest)

    val oauthParameterId = UUID.randomUUID()
    Mockito
      .`when`(
        oauthService.getSourceOAuthParamByDefinitionIdOptional(
          Optional.empty<UUID>(),
          Optional.empty<UUID>(),
          sourceDefId,
        ),
      ).thenReturn(Optional.of(SourceOAuthParameter().withOauthParameterId(oauthParameterId)))

    val secondParams: MutableMap<String?, Any?> = HashMap<String?, Any?>()
    secondParams.put(CLIENT_ID_KEY, "456")
    secondParams.put(CLIENT_SECRET_KEY, "hunter3")
    val secondRequest =
      SetInstancewideSourceOauthParamsRequestBody()
        .sourceDefinitionId(sourceDefId)
        .params(secondParams)
    handler.setSourceInstancewideOauthParams(secondRequest)

    val argument = argumentCaptor<SourceOAuthParameter>()
    Mockito.verify<OAuthService?>(oauthService, Mockito.times(2)).writeSourceOAuthParam(argument.capture())
    val capturedValues = argument.allValues
    Assertions.assertEquals(jsonNode<MutableMap<String?, Any?>?>(firstParams), capturedValues.get(0).configuration)
    Assertions.assertEquals(jsonNode<MutableMap<String?, Any?>?>(secondParams), capturedValues.get(1).configuration)
    Assertions.assertEquals(sourceDefId, capturedValues.get(0).sourceDefinitionId)
    Assertions.assertEquals(sourceDefId, capturedValues.get(1).sourceDefinitionId)
    Assertions.assertEquals(oauthParameterId, capturedValues.get(1).oauthParameterId)
  }

  @Test
  @Throws(IOException::class)
  fun setDestinationInstancewideOauthParams() {
    val destinationDefId = UUID.randomUUID()
    val params: MutableMap<String?, Any?> = HashMap<String?, Any?>()
    params.put(CLIENT_ID_KEY, CLIENT_ID)
    params.put(CLIENT_SECRET_KEY, CLIENT_SECRET)

    val actualRequest =
      SetInstancewideDestinationOauthParamsRequestBody()
        .destinationDefinitionId(destinationDefId)
        .params(params)

    handler.setDestinationInstancewideOauthParams(actualRequest)

    val argument = argumentCaptor<DestinationOAuthParameter>()
    Mockito.verify<OAuthService?>(oauthService).writeDestinationOAuthParam(argument.capture())
    Assertions.assertEquals(jsonNode<MutableMap<String?, Any?>?>(params), argument.firstValue.configuration)
    Assertions.assertEquals(destinationDefId, argument.firstValue.destinationDefinitionId)
  }

  @Test
  @Throws(IOException::class)
  fun resetDestinationInstancewideOauthParams() {
    val destinationDefId = UUID.randomUUID()
    val firstParams: MutableMap<String?, Any?> = HashMap<String?, Any?>()
    firstParams.put(CLIENT_ID_KEY, CLIENT_ID)
    firstParams.put(CLIENT_SECRET_KEY, CLIENT_SECRET)
    val firstRequest =
      SetInstancewideDestinationOauthParamsRequestBody()
        .destinationDefinitionId(destinationDefId)
        .params(firstParams)
    handler.setDestinationInstancewideOauthParams(firstRequest)

    val oauthParameterId = UUID.randomUUID()
    Mockito
      .`when`(
        oauthService.getDestinationOAuthParamByDefinitionIdOptional(
          Optional.empty<UUID>(),
          Optional.empty<UUID>(),
          destinationDefId,
        ),
      ).thenReturn(Optional.of(DestinationOAuthParameter().withOauthParameterId(oauthParameterId)))

    val secondParams: MutableMap<String?, Any?> = HashMap<String?, Any?>()
    secondParams.put(CLIENT_ID_KEY, "456")
    secondParams.put(CLIENT_SECRET_KEY, "hunter3")
    val secondRequest =
      SetInstancewideDestinationOauthParamsRequestBody()
        .destinationDefinitionId(destinationDefId)
        .params(secondParams)
    handler.setDestinationInstancewideOauthParams(secondRequest)

    val argument = argumentCaptor<DestinationOAuthParameter>()
    Mockito.verify<OAuthService?>(oauthService, Mockito.times(2)).writeDestinationOAuthParam(argument.capture())
    val capturedValues = argument.allValues
    Assertions.assertEquals(jsonNode<MutableMap<String?, Any?>?>(firstParams), capturedValues.get(0).configuration)
    Assertions.assertEquals(jsonNode<MutableMap<String?, Any?>?>(secondParams), capturedValues.get(1).configuration)
    Assertions.assertEquals(destinationDefId, capturedValues.get(0).destinationDefinitionId)
    Assertions.assertEquals(destinationDefId, capturedValues.get(1).destinationDefinitionId)
    Assertions.assertEquals(oauthParameterId, capturedValues.get(1).oauthParameterId)
  }

  @Test
  fun testBuildJsonPathFromOAuthFlowInitParameters() {
    val input =
      Map.ofEntries<String?, List<String>>(
        Map.entry<String?, List<String>>("field1", listOf<String>("1")),
        Map.entry<String?, List<String>>("field2", listOf<String>("2", "3")),
      )

    val expected =
      Map.ofEntries<String?, String?>(
        Map.entry<String?, String?>("field1", "$.1"),
        Map.entry<String?, String?>("field2", "$.2.3"),
      )

    Assertions.assertEquals(expected, handler.buildJsonPathFromOAuthFlowInitParameters(input))
  }

  @Test
  fun testGetOAuthInputConfiguration() {
    val hydratedConfig =
      deserialize(
        """
        {
          "field1": "1",
          "field2": "2",
          "field3": {
            "field3_1": "3_1",
            "field3_2": "3_2"
          }
        }
        
        """.trimIndent(),
      )

    val pathsToGet =
      Map.ofEntries<String?, String?>(
        Map.entry<String?, String?>("field1", "$.field1"),
        Map.entry<String?, String?>("field3_1", "$.field3.field3_1"),
        Map.entry<String?, String?>("field3_2", "$.field3.field3_2"),
        Map.entry<String?, String?>("field4", "$.someNonexistentField"),
      )

    val expected =
      deserialize(
        """
        {
          "field1": "1",
          "field3_1": "3_1",
          "field3_2": "3_2"
        }
        
        """.trimIndent(),
      )

    Assertions.assertEquals(expected, handler.getOAuthInputConfiguration(hydratedConfig, pathsToGet))
  }

  @Test
  fun testGetOauthFromDBIfNeeded() {
    val fromInput =
      deserialize(
        """
        {
          "testMask": "**********",
          "testNotMask": "this",
          "testOtherType": true
        }
        
        """.trimIndent(),
      )

    val fromDb =
      deserialize(
        """
        {
          "testMask": "mask",
          "testNotMask": "notThis",
          "testOtherType": true
        }
        
        """.trimIndent(),
      )

    val expected =
      deserialize(
        """
        {
          "testMask": "mask",
          "testNotMask": "this",
          "testOtherType": true
        }
        
        """.trimIndent(),
      )

    Assertions.assertEquals(expected, handler.getOauthFromDBIfNeeded(fromDb, fromInput))
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testCompleteSourceOAuthHandleReturnSecret() {
    val sourceDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    // This is being created without returnSecretCoordinate set intentionally
    val completeSourceOauthRequest =
      CompleteSourceOauthRequest()
        .sourceDefinitionId(sourceDefinitionId)
        .workspaceId(workspaceId)

    val handlerSpy: OAuthHandler = Mockito.spy<OAuthHandler>(handler)

    Mockito
      .doReturn(
        mapToCompleteOAuthResponse(Map.of<String, String?>("access_token", "access", "refresh_token", "refresh")),
      ).`when`<OAuthHandler>(handlerSpy)
      .completeSourceOAuth(any<CompleteSourceOauthRequest>())

    Mockito
      .doReturn(
        mapToCompleteOAuthResponse(Map.of<String, String?>("secret_id", "secret")),
      ).`when`<OAuthHandler>(handlerSpy)
      .writeOAuthResponseSecret(
        any<UUID>(),
        any<CompleteOAuthResponse>(),
      )

    handlerSpy.completeSourceOAuthHandleReturnSecret(completeSourceOauthRequest)

    Mockito.verify(handlerSpy).completeSourceOAuth(completeSourceOauthRequest)
    Mockito
      .verify(handlerSpy, Mockito.never())
      .writeOAuthResponseSecret(
        any<UUID>(),
        any<CompleteOAuthResponse>(),
      )

    completeSourceOauthRequest.returnSecretCoordinate(true)

    handlerSpy.completeSourceOAuthHandleReturnSecret(completeSourceOauthRequest)

    Mockito.verify(handlerSpy, Mockito.times(2)).completeSourceOAuth(completeSourceOauthRequest)
    Mockito
      .verify(handlerSpy)
      .writeOAuthResponseSecret(
        any<UUID>(),
        any<CompleteOAuthResponse>(),
      )

    completeSourceOauthRequest.returnSecretCoordinate(false)

    handlerSpy.completeSourceOAuthHandleReturnSecret(completeSourceOauthRequest)

    Mockito.verify(handlerSpy, Mockito.times(3)).completeSourceOAuth(completeSourceOauthRequest)
    Mockito
      .verify(handlerSpy)
      .writeOAuthResponseSecret(
        any<UUID>(),
        any<CompleteOAuthResponse>(),
      )
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testGetSourceOAuthParamConfigNoFeatureFlag() {
    val sourceDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val sourceOAuthParameter =
      SourceOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withSourceDefinitionId(sourceDefinitionId)
        .withConfiguration(
          deserialize(
            """
            {"credentials": {"client_id": "test", "client_secret": "shhhh" }}
            
            """.trimIndent(),
          ),
        )
    Mockito
      .`when`(
        oauthService.getSourceOAuthParameterWithSecretsOptional(
          any<UUID>(),
          any<UUID>(),
        ),
      ).thenReturn(Optional.of(sourceOAuthParameter))
    Mockito
      .`when`(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(any<JsonNode>()))
      .thenReturn(sourceOAuthParameter.getConfiguration())

    val expected =
      deserialize(
        """
        {"client_id": "test", "client_secret": "shhhh"}
        
        """.trimIndent(),
      )
    Assertions.assertEquals(expected, handler.getSourceOAuthParamConfig(workspaceId, sourceDefinitionId))
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testGetSourceOAuthParamConfigFeatureFlagNoOverride() {
    val sourceDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val sourceOAuthParameter =
      SourceOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withSourceDefinitionId(sourceDefinitionId)
        .withConfiguration(
          deserialize(
            """
            {"credentials": {"client_id": "test", "client_secret": "shhhh" }}
            
            """.trimIndent(),
          ),
        )
    Mockito
      .`when`(
        oauthService.getSourceOAuthParameterWithSecretsOptional(
          any<UUID>(),
          any<UUID>(),
        ),
      ).thenReturn(Optional.of(sourceOAuthParameter))
    Mockito
      .`when`(
        featureFlagClient.boolVariation(
          any(),
          any<Context>(),
        ),
      ).thenReturn(true)
    Mockito
      .`when`(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(any<JsonNode>()))
      .thenReturn(sourceOAuthParameter.getConfiguration())

    val expected =
      deserialize(
        """
        {"client_id": "test", "client_secret": "shhhh"}
        
        """.trimIndent(),
      )
    Assertions.assertEquals(expected, handler.getSourceOAuthParamConfig(workspaceId, sourceDefinitionId))
  }

  companion object {
    private const val CLIENT_ID = "123"
    private const val CLIENT_ID_KEY = "client_id"
    private const val CLIENT_SECRET_KEY = "client_secret"
    private const val CLIENT_SECRET = "hunter2"
  }
}
