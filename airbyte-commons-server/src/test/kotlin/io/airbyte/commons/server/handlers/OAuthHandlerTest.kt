/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

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
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.oauth.OAuthImplementationFactory
import io.airbyte.validation.json.JsonValidationException
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

internal class OAuthHandlerTest {
  private lateinit var secretStorageService: SecretStorageService
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
  private lateinit var secretReferenceService: SecretReferenceService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var metricClient: MetricClient
  private lateinit var featureFlagClient: FeatureFlagClient

  @BeforeEach
  fun init() {
    metricClient = mockk<MetricClient>()
    trackingClient = mockk<TrackingClient>()
    oauthImplementationFactory = mockk<OAuthImplementationFactory>()
    secretsRepositoryReader = mockk<SecretsRepositoryReader>()
    secretsRepositoryWriter = mockk<SecretsRepositoryWriter>()
    actorDefinitionVersionHelper = mockk<ActorDefinitionVersionHelper>()
    featureFlagClient = mockk<TestClient>()
    sourceService = mockk<SourceService>()
    destinationService = mockk<DestinationService>()
    oauthService = mockk<OAuthService>()
    secretPersistenceService = mockk<SecretPersistenceService>()
    secretReferenceService = mockk<SecretReferenceService>()
    secretStorageService = mockk<SecretStorageService>()
    workspaceService = mockk<WorkspaceService>()
    handler =
      OAuthHandler(
        oauthImplementationFactory,
        trackingClient,
        secretsRepositoryWriter,
        actorDefinitionVersionHelper,
        featureFlagClient,
        sourceService,
        destinationService,
        oauthService,
        secretPersistenceService,
        secretReferenceService,
        workspaceService,
        secretStorageService,
      )
  }

  @Test
  fun setSourceInstancewideOauthParams() {
    val sourceDefId = UUID.randomUUID()
    val params: MutableMap<String?, Any?> = HashMap()
    params.put(CLIENT_ID_KEY, CLIENT_ID)
    params.put(CLIENT_SECRET_KEY, CLIENT_SECRET)

    val actualRequest =
      SetInstancewideSourceOauthParamsRequestBody()
        .sourceDefinitionId(sourceDefId)
        .params(params)

    // Mock the method that's called inside setSourceInstancewideOauthParams
    every { oauthService.getSourceOAuthParamByDefinitionIdOptional(Optional.empty<UUID>(), Optional.empty<UUID>(), sourceDefId) } returns
      Optional.empty()
    every { oauthService.writeSourceOAuthParam(any<SourceOAuthParameter>()) } returns Unit

    handler.setSourceInstancewideOauthParams(actualRequest)

    val argumentSlot = slot<SourceOAuthParameter>()
    verify { oauthService.writeSourceOAuthParam(capture(argumentSlot)) }
    Assertions.assertEquals(jsonNode(params), argumentSlot.captured.configuration)
    Assertions.assertEquals(sourceDefId, argumentSlot.captured.sourceDefinitionId)
  }

  @Test
  fun setDestinationInstancewideOauthParams() {
    val destinationDefId = UUID.randomUUID()
    val params: MutableMap<String?, Any?> = HashMap()
    params.put(CLIENT_ID_KEY, CLIENT_ID)
    params.put(CLIENT_SECRET_KEY, CLIENT_SECRET)

    val actualRequest =
      SetInstancewideDestinationOauthParamsRequestBody()
        .destinationDefinitionId(destinationDefId)
        .params(params)

    // Mock the method that's called inside setDestinationInstancewideOauthParams
    every { oauthService.getDestinationOAuthParamByDefinitionIdOptional(Optional.empty<UUID>(), Optional.empty<UUID>(), destinationDefId) } returns
      Optional.empty()
    every { oauthService.writeDestinationOAuthParam(any<DestinationOAuthParameter>()) } returns Unit

    handler.setDestinationInstancewideOauthParams(actualRequest)

    val argumentSlot = slot<DestinationOAuthParameter>()
    verify { oauthService.writeDestinationOAuthParam(capture(argumentSlot)) }
    Assertions.assertEquals(jsonNode(params), argumentSlot.captured.configuration)
    Assertions.assertEquals(destinationDefId, argumentSlot.captured.destinationDefinitionId)
  }

  @Test
  fun testBuildJsonPathFromOAuthFlowInitParameters() {
    val input =
      mapOf(
        "field1" to listOf("1"),
        "field2" to listOf("2", "3"),
      )

    val expected =
      mapOf(
        "field1" to "$.1",
        "field2" to "$.2.3",
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
      mapOf(
        "field1" to "$.field1",
        "field3_1" to "$.field3.field3_1",
        "field3_2" to "$.field3.field3_2",
        "field4" to "$.someNonexistentField",
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
  fun testCompleteSourceOAuthHandleReturnSecret() {
    val sourceDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    // This is being created without returnSecretCoordinate set intentionally
    val completeSourceOauthRequest =
      CompleteSourceOauthRequest()
        .sourceDefinitionId(sourceDefinitionId)
        .workspaceId(workspaceId)

    val handlerSpy: OAuthHandler = spyk(handler)

    every { handlerSpy.completeSourceOAuth(any<CompleteSourceOauthRequest>()) } returns
      mapToCompleteOAuthResponse(mapOf("access_token" to "access", "refresh_token" to "refresh"))

    every { handlerSpy.writeOAuthResponseSecret(any<UUID>(), any<CompleteOAuthResponse>()) } returns
      mapToCompleteOAuthResponse(mapOf("secret_id" to "secret"))

    handlerSpy.completeSourceOAuthHandleReturnSecret(completeSourceOauthRequest)

    verify { handlerSpy.completeSourceOAuth(completeSourceOauthRequest) }
    verify(exactly = 0) { handlerSpy.writeOAuthResponseSecret(any<UUID>(), any<CompleteOAuthResponse>()) }

    completeSourceOauthRequest.returnSecretCoordinate(true)

    handlerSpy.completeSourceOAuthHandleReturnSecret(completeSourceOauthRequest)

    verify(exactly = 2) { handlerSpy.completeSourceOAuth(completeSourceOauthRequest) }
    verify { handlerSpy.writeOAuthResponseSecret(any<UUID>(), any<CompleteOAuthResponse>()) }

    completeSourceOauthRequest.returnSecretCoordinate(false)

    handlerSpy.completeSourceOAuthHandleReturnSecret(completeSourceOauthRequest)

    verify(exactly = 3) { handlerSpy.completeSourceOAuth(completeSourceOauthRequest) }
    verify { handlerSpy.writeOAuthResponseSecret(any<UUID>(), any<CompleteOAuthResponse>()) }
  }

  @Test
  fun testGetSourceOAuthParamConfig() {
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
    every {
      oauthService.getSourceOAuthParameterOptional(
        eq(workspaceId),
        eq(sourceDefinitionId),
      )
    } returns Optional.of(sourceOAuthParameter)

    every {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        any(),
        any(),
        any(),
      )
    } returns
      ConfigWithSecretReferences(sourceOAuthParameter.configuration, referencedSecrets = emptyMap())

    every {
      secretReferenceService.getHydratedConfiguration(any(), any())
    } returns sourceOAuthParameter.configuration

    val expected =
      deserialize(
        """
        {"client_id": "test", "client_secret": "shhhh"}
        """.trimIndent(),
      )
    Assertions.assertEquals(expected, handler.getSourceOAuthParameterConfigWithSecrets(workspaceId, sourceDefinitionId))
    verify(exactly = 1) {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        any(),
        any(),
        any(),
      )
    }
    verify(exactly = 1) {
      secretReferenceService.getHydratedConfiguration(any(), any())
    }
  }

  @Test
  fun testGetDestinationOAuthParamConfig() {
    val destinationDefinitionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val destinationOAuthParameter =
      DestinationOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withDestinationDefinitionId(destinationDefinitionId)
        .withConfiguration(
          deserialize(
            """
            {"credentials": {"client_id": "test", "client_secret": "shhhh" }}
            
            """.trimIndent(),
          ),
        )
    every {
      oauthService.getDestinationOAuthParameterWithSecretsOptional(
        any<UUID>(),
        any<UUID>(),
      )
    } returns Optional.of(destinationOAuthParameter)
    every {
      featureFlagClient.boolVariation(
        any(),
        any(),
      )
    } returns true
    every {
      oauthService.getDestinationOAuthParameterOptional(
        eq(workspaceId),
        eq(destinationDefinitionId),
      )
    } returns Optional.of(destinationOAuthParameter)

    every {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        any(),
        any(),
        any(),
      )
    } returns
      ConfigWithSecretReferences(destinationOAuthParameter.configuration, referencedSecrets = emptyMap())

    every {
      secretReferenceService.getHydratedConfiguration(any(), any())
    } returns destinationOAuthParameter.configuration

    val expected =
      deserialize(
        """
        {"client_id": "test", "client_secret": "shhhh"}
        
        """.trimIndent(),
      )
    Assertions.assertEquals(expected, handler.getDestinationOAuthParameterConfigWithSecrets(workspaceId, destinationDefinitionId))
    verify(exactly = 1) {
      secretReferenceService.getConfigWithSecretReferences(
        any(),
        any(),
        any(),
        any(),
      )
    }
    verify(exactly = 1) {
      secretReferenceService.getHydratedConfiguration(any(), any())
    }
  }

  companion object {
    private const val CLIENT_ID = "123"
    private const val CLIENT_ID_KEY = "client_id"
    private const val CLIENT_SECRET_KEY = "client_secret"
    private const val CLIENT_SECRET = "hunter2"
  }
}
